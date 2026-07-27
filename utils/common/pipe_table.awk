#!/usr/bin/awk -f
# pipe_table.awk
#
# Reformat pipe-delimited input into an aligned, pipe-delimited table
# (psql/markdown-style): each column is padded to the width of its widest
# value, columns are joined with " | ", and a "----+----" separator line
# is inserted after the first (header) row.
#
# There is no built-in bash/coreutils one-liner that produces this exact
# output: `column -t -s'|' -o' | '` can align the columns but has no way
# to insert a separator line under the header, so this is done in awk.
#
# Usage:
#   awk -f pipe_table.awk file.psv
#   cat file.psv | awk -f pipe_table.awk
#   ./pipe_table.awk file.psv               # if chmod +x'd
#
# Input:
#   Pipe-delimited text. The first line is treated as the header row.
# Output:
#   Pipe-delimited text, columns padded/aligned, " | " between columns,
#   and a "-----+-----" divider after the header row.
#
# Notes:
#   . Leading/trailing whitespace on each field is trimmed before measuring
#     width, so "1 | blah" and "1|blah" produce identical output.
#   . All columns are left-justified (no attempt is made to right-justify
#     numeric-looking columns).
#   . Uses a manual padding loop instead of printf's "%-*s" dynamic width
#     so this runs unmodified under gawk, mawk, and busybox awk.

BEGIN {
  FS = "|"
  nrows = 0
  ncols = 0
}

{
  nrows++
  if (NF > ncols) ncols = NF
  for (col = 1; col <= NF; col++) {
    val = $col
    gsub(/^[ \t]+|[ \t]+$/, "", val)
    data[nrows, col] = val
    if (length(val) > width[col]) width[col] = length(val)
  }
}

END {
  for (row = 1; row <= nrows; row++) {
    line = ""
    for (col = 1; col <= ncols; col++) {
      line = line pad(data[row, col], width[col])
      if (col < ncols) line = line " | "
    }
    print line

    # Separator line goes only after the first (header) row.
    if (row == 1) {
      sep = ""
      for (col = 1; col <= ncols; col++) {
        sep = sep dashes(width[col])
        if (col < ncols) sep = sep "-+-"
      }
      print sep
    }
  }
}

# Left-justify `val` in a field `width` characters wide.
function pad(val, w,    n, i, s) {
  s = val
  n = w - length(val)
  for (i = 0; i < n; i++) s = s " "
  return s
}

# Return a string of `n` dash characters.
function dashes(n,    i, s) {
  s = ""
  for (i = 0; i < n; i++) s = s "-"
  return s
}
