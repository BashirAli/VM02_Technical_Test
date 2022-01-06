"""
Constants file, hosting any hardcoded values which are required
Allows for re-usability of main code
"""

# minimum transaction amount which records should be filtered from
MIN_TRNSCTN_AMNT = '20'
# minimum year which records should be filtered from
MIN_YEAR = '2010-01-01 00:00:00 UTC'
# whether to skip the headers in the file or not when reading it in
skip_header_lines = True

# various bits of information of the input file
INPUT_METADATA = {
    'timestamp_col': 'timestamp',
    'timestamp_format': '%Y-%m-%d %H:%M:%S %Z',
    'origin_col': 'origin',
    'destination_col': 'destination',
    'transaction_col': 'transaction_amount'
}
