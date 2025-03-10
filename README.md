# Apex Trader Funding Payout Scraper

This project scrapes payout data from Apex Trader Funding's website to analyze trader performance and earnings. It uses parallel processing with adaptive batch sizing to efficiently collect data from multiple pages while respecting the website's resources.

## Features

- **Parallel Scraping**: Uses ThreadPoolExecutor to scrape multiple pages simultaneously
- **Adaptive Batch Sizing**: Automatically adjusts the number of parallel workers based on success/failure rates
- **Interactive Reports**: Generates HTML reports with filtering, sorting, and visualization capabilities
- **Standalone Reports**: Creates self-contained HTML reports that can be shared without CSV files
- **Resilient Processing**: Includes retry mechanisms and timeout handling to prevent getting stuck
- **Progress Tracking**: Shows real-time progress with ETA and interim reports

## Requirements

- Python 3.11+
- Chrome browser (for Selenium WebDriver)
- Poetry (for dependency management)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/apex-payout-scraper.git
   cd apex-payout-scraper
   ```

2. Install dependencies using Poetry:
   ```
   # Install Poetry if you don't have it
   # On Windows: (PowerShell)
   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

   # On macOS/Linux:
   curl -sSL https://install.python-poetry.org | python3 -

   # Install project dependencies
   poetry install
   ```

3. Alternatively, if you prefer using pip:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the scraper with Poetry:

```
poetry run python scrape_apex_payouts.py
```

Or if you're using a virtual environment with pip:

```
python scrape_apex_payouts.py
```

The script will:
1. Determine the total number of pages to scrape
2. Begin scraping pages in parallel with adaptive batch sizing
3. Generate interim reports every 10 successful pages
4. Create a final report when scraping is complete
5. Retry any failed pages at the end of the process

## Output Files

All output files are organized in dedicated folders:

### Data Files (in `data/` directory)
- `apex_payouts.csv`: Raw payout data with all records
- `aggregated_payouts.csv`: Aggregated data by trader name and location
- Interim versions of these files are also created during scraping

### Reports (in `reports/` directory)
- `payout_report.html`: Interactive HTML report with charts and filters
- `payout_report_standalone.html`: Self-contained version that can be shared without CSV files

## Report Features

The HTML reports include:
- Summary statistics about the scraping process
- Interactive filters by country, minimum earnings, and trader name
- Charts showing top traders and earnings by country
- Sortable table of all trader data
- Batch size adaptation history

## Customization

You can modify the following parameters in the script:
- `initial_batch_size`: Starting number of parallel workers (default: 10)
- `max_batch_size`: Maximum number of parallel workers (default: 100)
- `problematic_pages`: List of known problematic pages to skip

## Troubleshooting

- **Script gets stuck**: The script includes timeout detection and will cancel stuck tasks after 3 consecutive warnings
- **Chrome driver issues**: Make sure you have Chrome installed and updated
- **Rate limiting**: If you encounter rate limiting, reduce the `initial_batch_size` and `max_batch_size` values

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is for educational purposes only. Always respect website terms of service and robots.txt when scraping. Use responsibly and ethically.