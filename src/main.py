from dotenv import load_dotenv
load_dotenv()

import os
import sys
from aggregate import main
from datetime import datetime
import time

from logging_config import setup_logging
logger = setup_logging()

import tracemalloc
tracemalloc.start()
start = time.perf_counter()

start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

# Validate dates
if end_date and not start_date:
    raise Exception(f"End date cannot be provided without a start date.")


def parse_date(date_str):
    """Helper function to parse and validate a date string."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise Exception(f"Date '{date_str}' is not in the correct format (YYYY-MM-DD)."
        )

parsed_start_date = parse_date(start_date) if start_date else None
parsed_end_date = parse_date(end_date) if end_date else None

# Further validation for dates
if parsed_start_date and parsed_end_date and parsed_start_date > parsed_end_date:
    logger.warning("Start date is after the end date.")

logger.info(f"Starting aggregation for dates start_date: {start_date}, end_date: {end_date}")

# Pass the parsed dates to the main function
main(parsed_start_date, parsed_end_date)


end = time.perf_counter()
curr, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
    
def convert_size(size_bytes):
    # Handle the case for 0 bytes
    if size_bytes == 0:
        return "0B"
    
    # Define the units
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int((size_bytes).bit_length() - 1) // 10  # Find which unit to use
    p = 1024 ** i
    s = size_bytes / p
    return f"{s:.2f} {size_name[i]}"

converted_size = convert_size(peak)

logger.info(f"\nFinished in {end-start:.2f}sec.")
logger.info(f"Peak memory usage: {converted_size}.")
sys.exit("Exiting...")