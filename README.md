# Commission Calculation

A set of Python scripts for generating detailed and summary commission reports.

## Features
- Retrieve applies/orders data from the database
- Convert revenue using configurable exchange rates and split into positive/negative
- Call the WellProz API to fetch profit figures
- Apply various fee and markup rules
- Export `details.xlsx`, `summary.xlsx`, `well_proz_details.xlsx`, and `well_proz_summary.xlsx`

## Dependencies
- Python 3.8+
- pandas
- SQLAlchemy
- requests
- tqdm
- xlsxwriter
- ratelimit

## Usage
1. Ensure the SSL certificate `DigiCertGlobalRootCA.crt.pem` is placed in the script’s `BASE_DIR`.
2. Update database connection settings, date range, and other parameters in the script.
3. Run:
   ```bash
   python commission_calculation.py
