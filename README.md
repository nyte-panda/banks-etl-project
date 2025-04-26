# Largest Banks ETL Project

This project performs ETL (Extract, Transform, Load) operations on a dataset of the largest banks by market capitalization.  
It scrapes data from a saved archive of Wikipedia, transforms the data using live currency exchange rates, and loads the processed data into both a CSV file and a SQLite database.

## Project Structure

- **Extract:** Scrapes the table containing bank names and their market caps from the webpage.
- **Transform:** 
  - Reads exchange rates from a CSV file.
  - Calculates market caps in GBP, EUR, and INR currencies.
- **Load:**
  - Saves the transformed data into a CSV file.
  - Loads the data into a SQLite database.
  - Runs a SQL query to select banks with a market capitalization greater than or equal to $10 billion USD.

## Files

- `banks_project.py` - Main ETL script.
- `exchange_rates.csv` - CSV file containing currency exchange rates.
- `Largest_banks_data.csv` - Output CSV file with the final transformed data.
- `Banks.db` - SQLite database containing the final table.
- `code_log.txt` - Log file recording the ETL progress.

## How to Run

1. Clone the repository.
2. Install required Python packages (if not already installed):
   ```bash
   pip install pandas beautifulsoup4 requests
