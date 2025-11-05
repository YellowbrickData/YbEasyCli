#!/usr/bin/env python3
"""
Utilities for splitting large polygon geometries into smaller parts that
conform to a maximum Well-Known Text (WKT) length, optionally consolidating
smaller parts back into valid MultiPolygons, and loading both original
metadata and split parts into Yellowbrick tables.

Overview
--------
- Reads polygon WKT values from a source SQLite database (via a user-provided
  SQL query returning two columns: an identifier and a WKT string).
- Iteratively splits large polygons using axis-aligned bisection of the
  bounding box until each part's WKT length is <= a user-specified maximum.
- Optionally consolidates small parts into MultiPolygons without exceeding the
  max WKT length, using a greedy packing strategy.
- Computes geodesic areas using pyproj.Geod on the WGS84 ellipsoid for both
  originals and parts, and writes results to two Yellowbrick tables.

Dependencies
------------
- shapely: parsing WKT, geometric ops (intersection/difference, bounds).
- pyproj: accurate geodesic area computations (WGS84 ellipsoid).
- SpatiaLite (optional in SQLite): enables geospatial operations if needed by
  the source workflow.

Dependency Installation
-----------------------
Python libraries (choose one method):
- pip:
  pip install shapely pyproj
- conda (conda-forge recommended):
  conda install -c conda-forge shapely pyproj

SpatiaLite (optional, only if you need SQLite geospatial functions):
- Windows:
  - Install a SpatiaLite build (e.g., via OSGeo4W) or obtain mod_spatialite.dll.
  - Provide --spatialite-path with the full path to mod_spatialite.dll and ensure
    its directory is present in PATH (the script temporarily prepends it if needed).
- macOS (Homebrew):
  brew install libspatialite
  Then load by name (mod_spatialite) or provide the full .dylib path via --spatialite-path.
- Debian/Ubuntu:
  sudo apt-get update && sudo apt-get install -y libsqlite3-mod-spatialite
  Then load by name (mod_spatialite) or provide the full .so path via --spatialite-path.

Example Source DB (Natural Earth)
---------------------------------
Use the prebuilt Natural Earth SQLite database archive: `natural_earth_vector.sqlite.zip`.
Unzip to get `natural_earth_vector.sqlite` (contains Natural Earth vector layers).

Sample args file: `split_polygon_example.args`
- Sets connection flags, points `--source-db-file` to the Natural Earth DB, and
  provides a `--source-query` example filtering a single country. It also shows
  `--spatialite-path` usage on Windows.

Excerpt of relevant lines in the args file:
  --source-db-file "C:\\path\\to\\natural_earth_vector.sqlite"
  --source-query "SELECT name AS id, ST_AsText(ST_GeomFromWKB(GEOMETRY)) AS wkt FROM ne_10m_admin_0_countries WHERE name = 'United States of America'"
  --spatialite-path "C:\\path\\to\\mod_spatialite.dll"
  --dest-table public.country_polygons_b
  --max-polygon-len 200000
  --insert-batch-size 100

Notes:
- If your table already stores WKT text (e.g., a `wkt` column), you can use a simpler query:
    SELECT iso_a3 AS id, wkt AS wkt FROM ne_10m_admin_0_countries
- If geometry is stored in a geometry/WKB column, use SpatiaLite functions as shown in the args file.

Outputs
-------
- <dest-table>: one row per original geometry with area, WKT/WKB lengths, and
  the number of generated chunks.
- <dest-table>_split: one row per split part, including per-part area and
  the GEOGRAPHY value constructed from WKT.

Usage (examples)
----------------
Direct flags:
    yb_split_polygon.py \
      @$HOME/conn.args \
      --source-db-file /path/to/source.db \
      --source-query "SELECT id, wkt FROM large_geometries" \
      --dest-table public.my_polygons \
      --max-polygon-len 4000 \
      --execute

Using the provided args file (adjust paths inside if needed):
    yb_split_polygon.py @split_polygon_example.args --execute

Notes
-----
- The splitting heuristic is simple and robust for many real-world cases but
  does not attempt optimal piece minimization; it prioritizes staying under
  the WKT size threshold.
- Area differences between originals and the sum of parts are reported to help
  assess numeric/representation effects.
"""
import os
import sqlite3
import sys

# Add the 'bin' directory to the Python path to find yb_common
script_dir = os.path.dirname(os.path.realpath(__file__))
common_dir = os.path.abspath(os.path.join(script_dir, '..', 'bin'))
if common_dir not in sys.path:
    sys.path.insert(0, common_dir)

from yb_common import Common, Util, DBConnect

try:
    from shapely.geometry import shape, MultiPolygon, Polygon, box
    from shapely.wkt import loads as wkt_loads
except ImportError as e:
    Common.error(
        f"{e}\n\nThe 'shapely' library is required. Please install it using: "
        "'pip install shapely'\n"
    )
try:
    from pyproj import Geod
except ImportError as e:
    Common.error(
        f"{e}\n\nThe 'pyproj' library is required for area calculations. Please install it using: "
        "'pip install pyproj'\n"
    )

class YBSplitPolygon(Util):
    """
    Command-line utility for splitting polygon geometries and loading results
    into Yellowbrick.

    This tool reads identifier/WKT pairs from a source SQLite database query,
    splits large polygons into smaller parts whose WKT length does not exceed
    a provided threshold, optionally consolidates smaller parts into valid
    MultiPolygons, computes geodesic areas for originals and parts, and
    writes results into two destination tables: the original metadata table
    and a companion "_split" table containing individual parts.
    """
    config = {
        'description': (
            'Reads large polygons from a source DB, splits them using a geometric algorithm, '
            'and inserts the original polygon metadata and split parts into two Yellowbrick tables.\n\n'
            'Two tables will be created/populated: <dest-table> and <dest-table>_split.'
        ),
        'optional_args_single': [],
        'usage_example': {
            'cmd_line_args': (
                '@$HOME/conn.args @split_polygon_example.args --execute\n\n'
                'Example $HOME/conn.args:\n'
                '  -h <host>\n'
                '  -U <user>\n'
                '  -d <database>\n'
                '  # optional: -p <port>\n\n'
                'Example split_polygon_example.args:\n'
                '  --source-db-file "C:\\path\\to\\natural_earth_vector.sqlite"\n'
                '  --source-query "SELECT name AS id, ST_AsText(ST_GeomFromWKB(GEOMETRY)) AS wkt FROM ne_10m_admin_0_countries WHERE name = \'United States of America\'"\n'
                '  --spatialite-path "C:\\path\\to\\mod_spatialite.dll"\n'
                '  --dest-table public.country_polygons_b\n'
                '  --max-polygon-len 200000\n'
                '  --insert-batch-size 100'
            )
        }
    }

    def additional_args(self):
        """
        Register CLI arguments for source acquisition, splitting, and destination.

        Argument groups
        - source database arguments:
          --source-db-file: path to a SQLite file containing the source data.
          --source-query: SQL expected to return exactly two columns: (id, wkt).
          --spatialite-path: optional path to SpatiaLite module for enabling
            geospatial functions via SQLite extensions.

        - splitting arguments:
          --max-polygon-len: maximum allowed WKT string length for any output
            polygon or MultiPolygon part.

        - destination arguments:
          --dest-table: base table name. A companion "_split" table is used for
            part rows.
          --execute: if provided, execute INSERTs; otherwise print SQL.
          --insert-batch-size: number of rows per batch insert when executing.
        """
        source_grp = self.args_handler.args_parser.add_argument_group(
            'source database arguments')
        source_grp.add_argument(
            '--source-db-file', required=True,
            help='Path to the source SQLite database file')
        source_grp.add_argument(
            '--source-query', required=True,
            help='SQL query to fetch data from the source. Must return two columns: id and wkt_string')
        source_grp.add_argument(
            '--spatialite-path',
            help='Path to the SpatiaLite module (e.g., C:\\path\\to\\mod_spatialite.dll)')

        split_grp = self.args_handler.args_parser.add_argument_group(
            'splitting arguments')
        split_grp.add_argument(
            '--max-polygon-len', required=True, type=int,
            help='Maximum character length for a single output polygon WKT string')

        dest_grp = self.args_handler.args_parser.add_argument_group(
            'destination arguments')
        dest_grp.add_argument(
            '--dest-table', required=True,
            help='Base name for destination tables in Yellowbrick (e.g., my_db.public.my_polygons). '
                 'Two tables will be created/populated: <dest-table> and <dest-table>_split.')
        dest_grp.add_argument(
            '--execute', action="store_true",
            help='Execute the INSERT statements on the destination DB. If the destination tables do not exist, they will be created. '
                 'Default is to print the INSERT statements to stdout.')
        dest_grp.add_argument(
            '--insert-batch-size', type=int, default=1,
            help='Number of rows to insert in a single batch INSERT statement. Default is 1 (no batching).')

    def split_polygon(self, wkt_string, max_len):
        """
        Split a polygon or multipolygon WKT into parts whose WKT length <= max_len.

        Strategy
        - Parses the input WKT using shapely.
        - For MultiPolygons: decompose into individual Polygons and process each.
        - For Polygons: bisect the bounding box along its longer axis using an
          axis-aligned rectangular blade, then enqueue the two resulting pieces.
        - Repeat until every queued part's WKT length is <= max_len.

        Args
        - wkt_string: WKT representation of a Polygon or MultiPolygon.
        - max_len: maximum allowed length of each output part's WKT string.

        Returns
        - List[str]: WKT strings for all parts meeting the size constraint.

        Notes
        - The heuristic is greedy and does not guarantee the fewest possible parts.
        - Geometry validity is preserved by using shapely intersection/difference.
        """
        final_parts = []
        work_queue = [wkt_string]

        while work_queue:
            current_wkt = work_queue.pop(0)

            # Base Case: If the polygon is small enough, add it to the final list.
            if len(current_wkt) <= max_len:
                final_parts.append(current_wkt)
                continue

            try:
                geom = wkt_loads(current_wkt)
            except Exception as e:
                print(f"Warning: Could not parse WKT, skipping. Error: {e}\nWKT: {current_wkt[:100]}...")
                continue

            # If it's a MultiPolygon, decompose it and add individual Polygons to the queue.
            if isinstance(geom, MultiPolygon):
                for poly in geom.geoms:
                    work_queue.append(poly.wkt)
                continue

            # If it's a Polygon, perform the split.
            if isinstance(geom, Polygon):
                min_x, min_y, max_x, max_y = geom.bounds
                # Prefer bisecting along the longer bbox edge to reduce size faster
                if (max_x - min_x) > (max_y - min_y):  # Split vertically
                    mid_x = (min_x + max_x) / 2
                    blade_poly = box(min_x, min_y, mid_x, max_y)
                else:  # Split horizontally
                    mid_y = (min_y + max_y) / 2
                    blade_poly = box(min_x, min_y, max_x, mid_y)

                part1 = geom.intersection(blade_poly)
                part2 = geom.difference(blade_poly)

                if not part1.is_empty:
                    work_queue.append(part1.wkt)
                if not part2.is_empty:
                    work_queue.append(part2.wkt)

        return final_parts

    def consolidate_polygons(self, wkt_parts, max_len):
        """
        Greedily pack small polygon WKTs into MultiPolygons under a WKT size cap.

        Given parts that already satisfy the size threshold individually, this
        routine attempts to merge them into MultiPolygons while keeping the
        combined WKT length <= max_len. It sorts by WKT length ascending and
        tries to add geometries into the current group if the resulting combined
        WKT remains within the limit.

        Args
        - wkt_parts: list of WKT strings for Polygon or MultiPolygon parts.
        - max_len: maximum allowed WKT length for any consolidated output.

        Returns
        - List[str]: WKT strings for consolidated MultiPolygons (or single
          Polygons if no consolidation was possible/needed).
        """
        if not wkt_parts:
            return []

        # Create a list of (geometry, wkt_length) tuples and sort by length
        geoms_with_len = []
        for wkt in wkt_parts:
            try:
                geom = wkt_loads(wkt)
                geoms_with_len.append((geom, len(wkt)))
            except Exception as e:
                print(f"Warning: Could not parse part for consolidation, skipping. Error: {e}")
        
        # Sort by WKT length, smallest first
        geoms_with_len.sort(key=lambda x: x[1])

        consolidated_wkts = []
        
        while geoms_with_len:
            # Start a new group with the smallest remaining polygon
            current_geoms = [geoms_with_len.pop(0)[0]]
            
            # Keep track of which items to remove from the main list
            indexes_to_remove = []
            for i, (geom, _) in enumerate(geoms_with_len):
                # Try adding the next geometry to the current group
                
                # Flatten the list of geometries to handle nested MultiPolygons
                flat_test_group = []
                for g in (current_geoms + [geom]):
                    if isinstance(g, MultiPolygon):
                        flat_test_group.extend(list(g.geoms))
                    elif isinstance(g, Polygon):
                        flat_test_group.append(g)

                combined_geom = MultiPolygon(flat_test_group)
                
                if len(combined_geom.wkt) <= max_len:
                    # If it fits, add it to the group and mark for removal
                    current_geoms.append(geom)
                    indexes_to_remove.append(i)
            
            # Remove the consolidated geometries from the main list in reverse order
            for i in sorted(indexes_to_remove, reverse=True):
                geoms_with_len.pop(i)

            # Finalize the current group and add its WKT to the results
            flat_final_group = []
            for g in current_geoms:
                if isinstance(g, MultiPolygon):
                    flat_final_group.extend(list(g.geoms))
                elif isinstance(g, Polygon):
                    flat_final_group.append(g)

            final_geom = MultiPolygon(flat_final_group) if len(flat_final_group) > 1 else flat_final_group[0]
            consolidated_wkts.append(final_geom.wkt)

        return consolidated_wkts

    def execute(self):
        """
        Execute the end-to-end workflow: read, split, consolidate, compute areas,
        and load/emit SQL for Yellowbrick destination tables.

        Steps
        1) Connect to the source SQLite database (optionally loading SpatiaLite).
        2) Prepare destination table names (<dest>, <dest>_split); create them
           if --execute is provided and they do not exist.
        3) For each (id, wkt) row from --source-query:
           - Parse original geometry and compute geodesic area (WGS84).
           - Split the geometry to respect --max-polygon-len.
           - Consolidate small parts where possible.
           - Insert one row into <dest> with metadata and chunk count.
           - Insert one row per part into <dest>_split with GEOGRAPHY values.
        4) When not in --execute mode, print SQL instead of executing.

        Side effects
        - Emits progress and summary information to stdout, including aggregate
          area differences to help evaluate numeric effects of splitting.
        """
        args = self.args_handler.args

        geod = Geod(ellps='WGS84') # For geodesic area calculations in square meters

        # --- Destination Yellowbrick Table Setup ---
        dest_table_original_name = args.dest_table
        dest_table_split_name = f"{args.dest_table}_split"

        if not os.path.exists(args.source_db_file):
            Common.error(f"Source database file not found: {args.source_db_file}")

        source_conn = None
        try:
            source_conn = sqlite3.connect(args.source_db_file)
            # Enable loading of extensions
            source_conn.enable_load_extension(True)

            # Load the SpatiaLite extension to enable geospatial functions
            spatialite_path = args.spatialite_path
            if spatialite_path:
                # On Windows, we need to help Python find the dependency DLLs.
                # The most reliable way is to temporarily add the DLL's directory to the system PATH.
                if sys.platform == 'win32':
                    dll_dir = os.path.dirname(spatialite_path)
                    if os.path.isdir(dll_dir):
                        print(f"Temporarily adding '{dll_dir}' to PATH to find SpatiaLite dependencies.")
                        os.environ['PATH'] = dll_dir + os.pathsep + os.environ.get('PATH', '')
                    else:
                        Common.error(f"Directory for SpatiaLite path does not exist: '{dll_dir}'")
                try:
                    source_conn.load_extension(spatialite_path)
                except sqlite3.OperationalError as e:
                    Common.error(f"Failed to load SpatiaLite from path '{spatialite_path}': {e}")
            else:
                # Try loading with default names if no path is provided
                try:
                    source_conn.load_extension('mod_spatialite')
                except sqlite3.OperationalError:
                    try:
                        source_conn.load_extension('mod_spatialite.dll') # Fallback for Windows
                    except sqlite3.OperationalError as e:
                        Common.error(
                            f"Failed to load SpatiaLite extension: {e}\n"
                            "Ensure SpatiaLite is installed and its location is in your system's PATH,\n"
                            "or provide the full path to the module using the --spatialite-path argument.")

            source_cursor = source_conn.cursor()
            print(f"Successfully connected to source database: {args.source_db_file}")
        except sqlite3.Error as e:
            Common.error(f"Error connecting to source database: {e}")

        try:
            total_original_polygons = 0
            total_split_parts = 0
            total_original_area = 0.0
            total_split_area_sum = 0.0

            # List to hold values for batch inserts
            split_values_batch = []

            if args.execute:
                # Check for and create tables if --execute is specified
                print(f"Execution mode enabled. Checking/creating destination tables...")
                
                # Check for the main metadata table by trying to query it.
                check_sql = f"SELECT 1 FROM {dest_table_original_name} LIMIT 1;"
                cmd_result = self.db_conn.ybsql_query(check_sql)
                # On Windows, exit_code can be 0 even if ybsql fails. Check stderr for 'ERROR:' as well.
                if (cmd_result.exit_code != 0 or 'ERROR:' in cmd_result.stderr) and 'does not exist' in cmd_result.stderr:
                    print(f"Table '{dest_table_original_name}' not found, creating it.")
                    create_original_table_sql = f"""
                        CREATE TABLE {dest_table_original_name} (
                            id VARCHAR(256) PRIMARY KEY,
                            area DOUBLE PRECISION,
                            wkt_length BIGINT,
                            wkb_length BIGINT,
                            chunk_ct BIGINT
                        );
                    """
                    self.db_conn.ybsql_query(create_original_table_sql).on_error_exit()
                else:
                    if cmd_result.exit_code == 0:
                        print(f"Table '{dest_table_original_name}' already exists.")
                    else:
                        cmd_result.on_error_exit() # Exit if there was a different, unexpected error

                # Check for the split geometry table by trying to query it.
                check_sql = f"SELECT 1 FROM {dest_table_split_name} LIMIT 1;"
                cmd_result = self.db_conn.ybsql_query(check_sql)
                # On Windows, exit_code can be 0 even if ybsql fails. Check stderr for 'ERROR:' as well.
                if (cmd_result.exit_code != 0 or 'ERROR:' in cmd_result.stderr) and 'does not exist' in cmd_result.stderr:
                    print(f"Table '{dest_table_split_name}' not found, creating it.")
                    create_split_table_sql = f"""
                        SET enable_geospatial = on; CREATE TABLE {dest_table_split_name} (
                            id VARCHAR(256),
                            chunk_id BIGINT,
                            area DOUBLE PRECISION,
                            wkt_length BIGINT,
                            wkb_length BIGINT,
                            geometry GEOGRAPHY
                        );
                    """
                    self.db_conn.ybsql_query(create_split_table_sql).on_error_exit()
                else:
                    if cmd_result.exit_code == 0:
                        print(f"Table '{dest_table_split_name}' already exists.")
                    else:
                        cmd_result.on_error_exit() # Exit if there was a different, unexpected error

            source_cursor.execute(args.source_query)
            for row in source_cursor:
                if len(row) != 2:
                    Common.error("Source query must return exactly two columns: an ID and a WKT string.")

                char_id, wkt_string = row[0], row[1]
                print(f"Processing polygon with ID: {char_id}")

                # Escape single quotes in the ID for SQL safety
                safe_char_id = char_id.replace("'", "''")

                # Calculate original area
                try:
                    original_geom = wkt_loads(wkt_string)
                    # Calculate geodesic area in square meters
                    original_area = abs(geod.geometry_area_perimeter(original_geom)[0])
                except Exception as e:
                    print(f"  Warning: Could not parse original WKT to get area, skipping. Error: {e}")
                    continue

                # Split the polygon
                initial_split_parts = self.split_polygon(wkt_string, args.max_polygon_len)

                # Consolidate small polygons into MultiPolygons
                split_parts = self.consolidate_polygons(initial_split_parts, args.max_polygon_len)

                # Insert original polygon metadata, now including the chunk count
                insert_original_sql = f"""
                    INSERT INTO {dest_table_original_name} (id, area, wkt_length, wkb_length, chunk_ct)
                    VALUES ('{safe_char_id}', {original_area}, {len(wkt_string)}, {len(original_geom.wkb)}, {len(split_parts)});
                """
                if args.execute:
                    self.db_conn.ybsql_query(insert_original_sql).on_error_exit()
                else:
                    print(insert_original_sql)

                # Process and insert split parts
                chunk_id_counter = 0
                current_split_area_sum = 0.0
                for part_wkt in split_parts:
                    chunk_id_counter += 1
                    try:
                        split_geom = wkt_loads(part_wkt)
                        # Calculate geodesic area in square meters
                        split_area = abs(geod.geometry_area_perimeter(split_geom)[0])
                        split_wkb_len = len(split_geom.wkb)
                        current_split_area_sum += split_area
                    except Exception:
                        split_area = 'NULL'
                        split_wkb_len = 'NULL'

                    safe_wkt = part_wkt.replace("'", "''")
                    insert_split_sql = f"""
                        SET enable_geospatial = on; INSERT INTO {dest_table_split_name} (id, chunk_id, area, wkt_length, wkb_length, geometry) VALUES 
                    """
                    if args.execute:
                        # Batch INSERTs for execution mode
                        value_tuple = f"('{safe_char_id}', {chunk_id_counter}, {split_area}, {len(part_wkt)}, {split_wkb_len}, ST_GeogFromText('{safe_wkt}'))"
                        split_values_batch.append(value_tuple)

                        if len(split_values_batch) >= args.insert_batch_size:
                            print(f"  -> Inserting batch of {len(split_values_batch)} rows...")
                            batch_sql = insert_split_sql + ',\n'.join(split_values_batch) + ';'
                            self.db_conn.ybsql_query(batch_sql).on_error_exit()
                            split_values_batch = [] # Reset batch
                    else:
                        # Print single INSERTs for non-execution mode
                        single_insert = insert_split_sql + f"('{safe_char_id}', {chunk_id_counter}, {split_area}, {len(part_wkt)}, {split_wkb_len}, ST_GeogFromText('{safe_wkt}'));"
                        print(single_insert)

                print(f"  -> Generated {len(split_parts)} parts. Area diff: {original_area - current_split_area_sum:.4f}")
                total_original_polygons += 1
                total_split_parts += len(split_parts)
                total_original_area += original_area
                total_split_area_sum += current_split_area_sum

            # Insert any remaining rows in the final batch
            if args.execute and split_values_batch:
                print(f"  -> Inserting final batch of {len(split_values_batch)} rows...")
                batch_sql = f"INSERT INTO {dest_table_split_name} (id, chunk_id, area, wkt_length, wkb_length, geometry) VALUES " + ',\n'.join(split_values_batch) + ';'
                self.db_conn.ybsql_query(batch_sql).on_error_exit()

            overall_area_diff = total_original_area - total_split_area_sum
            percentage_diff = (overall_area_diff / total_original_area * 100) if total_original_area > 0 else 0.0

            print("\nProcessing complete.")
            print(f"Total original polygons processed: {total_original_polygons}")
            print(f"Total split parts generated: {total_split_parts}")
            print(f"Total Original Area Sum: {total_original_area:.4f}")
            print(f"Total Split Parts Area Sum: {total_split_area_sum:.4f}")
            print(f"Overall Area Difference: {overall_area_diff:.4f} ({percentage_diff:+.4f}%)")
            if args.execute:
                print(f"Data inserted into destination tables: {dest_table_original_name} and {dest_table_split_name}")
            else:
                print("Run with --execute to insert data into the destination database.")

        finally:
            if source_conn:
                source_conn.close()

if __name__ == "__main__":
    YBSplitPolygon().execute()