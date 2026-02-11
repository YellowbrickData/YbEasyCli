# db_char_cols.sh
# 
# Iterate over all user databases and run the CHAR column SQL files.
# There are multiple files that can be run for different types of checks.
# . db_char_tbl_cols_smry.sql - User table CHAR column in the current db.
# . db_char_tbl_cols.sql      - User table CHAR column in the current db.
# . db_bpchar_fn_smry.sql     - YB_BPCHAR_RTRIM1 and YB_BPCHAR_RPAD1 plan usage in curr db.
# . db_bpchar_stmts.sql       - All stmts with YB_BPCHAR_RTRIM1 or YB_BPCHAR_RPAD1 in plan.
# 
# Prerequistes:
# . Expected to be run as superuser. Otherwise only the tables the connected 
#   user has access to will be found.
# . YBUSER & YBHOST env variables set if not run from manager node
# . YBPASSWORD env variable set if not run from manager node unless you want to
#   keep renetering password.
#
# Dependencies:
# . db_char_columns.sql file in the same directory as this script
#
# Revision:
# . 2026.02.10 (rek&em) - better handling of db names with special chars
# . 2026.01.20 (rek)    - Revision to also handle CHAR table cols summary (IN PROCESS)
# . 2025.03.16 (rek)    - Inital version.
#
# TODO:
# . implement handling for #db_char_tbl_cols db_bpchar_fn_smry db_bpchar_stmts
#

echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
  
outdir="${1:-.}"
pad_char=" "

mkdir -p ${outdir} || (echo "Could not create output directory '${outdir}'. Exiting" >&2 && exit 1)

# Get the widths of the widest db name for formatting output.
dflt_db_nm_wdth=$( ybsql -d yellowbrick -qAtc "SELECT MAX( LENGTH(name))+1 FROM sys.database" );

# Currently only running the check for CHAR table colums summary
for db_check_type in db_char_tbl_cols_smry  #db_char_tbl_cols db_bpchar_fn_smry db_bpchar_stmts
do
  num_dbs=0
  num_dbs_w_chars=0
  num_files=0
  num_char_tables=0
  num_char_cols=0
  echo "db_check_type=${db_check_type}"

  # db_clean_name replaces non-word chars with an "_" to prevent problem bash file names
  dbs_sql="SELECT name, REGEXP_REPLACE(name, '[^a-zA-Z0-9_]', '_', 'g') AS db_clean_name FROM sys.database order by 1"
  ybsql -XAqt -c "${dbs_sql}"  while IFS='|' read db db_clean_name
  do
    # This handles
            
    # db_char_tbl_cols_smry is a special case becuase it is only summary data 
    # All the output should be a single file genereated by the script.
    if [ "${db_check_type}" == "db_char_tbl_cols_smry" ]
    then
      outfile="${outdir}/${db_check_type}.out"
      printf "%-${dflt_db_nm_wdth}s|%11s|%8s\n" \
             "database_name" "char_tables" "char_cols" \
             > ${outfile}
        
      while IFS="|" read -r db_name char_tables char_cols
      do
        char_tables=${char_tables:-0}
        char_cols=${char_cols:-0}
    
        ((num_dbs++))
        [[ ${char_tables} -ne 0 ]] && ((num_dbs_w_chars++))
        ((num_char_tables += ${char_tables}))
        ((num_char_cols += ${char_cols}))
        
        printf "%-${dflt_db_nm_wdth}s|%11d|%8d\n" \
               "${db_name}" "${char_tables}" "${char_cols}" \
               > ${outfile}
             
      done < <(ybsql -d "${db}" -qAtXf "${db_check_type}.sql")
      
      echo "num_dbs=${num_dbs}, num_dbs_w_chars=${num_dbs_w_chars} "\
         ", num_char_tables=${num_char_tables}, num_char_cols=${num_char_cols}" \
         > "${outdir}/${db_check_type}_aggr.out"
      ((num_files++))
      
    else # Not implemented
      ((num_dbs++))
      ((num_files++))

      outfile="${outdir}/${db_clean_name}_${db_output_type}.out"
      printf "[${db_output_type}] DB: %-${dflt_db_nm_wdth}.${dflt_db_nm_wdth}s" "${db//$pad_char/ }"
      printf "OUTPUT: '${outfile}'\n"
      if [ "${db_output_type}" == "bp_char_stmts" ]
      then
        ybsql -d "${db}" -f db_${db_output_type}.sql  -o ${outfile}
      else 
        ybsql -d "${db}" -f db_${db_output_type}.sql  -o ${outfile} 
      fi
          
      outfile="${outdir}/${db_clean_name}_${db_output_type}.out"
      printf "[${db_output_type}] DB: %-${dflt_db_nm_wdth}.${dflt_db_nm_wdth}s" "${db//$pad_char/ }"
      printf "OUTPUT: '${outfile}'\n"
      ybsql -d "${db}" -f db_${db_output_type}.sql  -o ${outfile}
      
    fi
  
  done 
  
  echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
done

echo ""
echo "DONE. Databases=${num_dbs}, Files=${num_files}, Output Files are:"
ls -1 -d ./${outdir}/*
echo ""
