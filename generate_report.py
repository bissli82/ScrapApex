import pandas as pd
import json
from datetime import datetime
import os

# Create reports directory if it doesn't exist
os.makedirs('reports', exist_ok=True)

def generate_html_report(csv_file='data/aggregated_payouts.csv', successful_pages=None, total_pages=None, 
                        start_date=None, end_date=None, failed_pages=None, is_interim=False, 
                        current_progress=None, batch_size_history=None, current_batch_size=None,
                        df=None, embed_data=False):
    """Generate an HTML report of the scraping results
    
    If embed_data is True, the data will be embedded in the HTML file,
    making it self-contained and shareable without needing the CSV files.
    """
    
    # Check if DataFrame is provided directly
    if df is not None:
        has_data = not df.empty
    else:
        # Try to read from CSV file
        try:
            # Read the aggregated CSV file
            df = pd.read_csv(csv_file)
            has_data = True
        except (FileNotFoundError, pd.errors.EmptyDataError):
            has_data = False
            df = pd.DataFrame()
    
    # Extract country from Location (assuming format "State, Country" or just "Country")
    if has_data and 'Location' in df.columns:
        df['Country'] = df['Location'].apply(lambda x: x.split(',')[-1].strip() if isinstance(x, str) and ',' in x else x)
    
    # Convert the DataFrame to JSON for JavaScript
    data_json = "[]"
    if has_data and not df.empty:
        data_json = df.to_json(orient='records')
    
    # Get unique countries for dropdown
    countries = []
    if has_data and 'Country' in df.columns:
        countries = sorted(df['Country'].unique())
    
    # Generate country options HTML
    country_options = '\n'.join([f'<option value="{country}">{country}</option>' for country in countries])
    
    # Format date range string
    date_range = ""
    if start_date and end_date:
        date_range = f"Report represents data from {end_date.strftime('%B %d, %Y')} to {start_date.strftime('%B %d, %Y')}"
    
    # Format batch size history
    batch_size_html = ""
    if batch_size_history:
        batch_size_html = """
        <div class="batch-size-history">
            <h3>Batch Size Adaptation</h3>
            <p>Current batch size: <strong>{}</strong></p>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Pages Completed</th>
                        <th>Batch Size</th>
                    </tr>
                </thead>
                <tbody>
        """.format(current_batch_size)
        
        for pages_completed, batch_size in batch_size_history:
            batch_size_html += f"""
                <tr>
                    <td>{pages_completed}</td>
                    <td>{batch_size}</td>
                </tr>
            """
        
        batch_size_html += """
                </tbody>
            </table>
        </div>
        """
    
    # JavaScript code for the interactive features
    js_code = """
    // Store the data in JavaScript
    const tradersData = REPLACE_WITH_DATA_JSON;
    let filteredData = [...tradersData];
    
    // Function to update the table with filtered data
    function updateTable() {
        const tableBody = document.getElementById('tableBody');
        tableBody.innerHTML = '';
        
        // Sort by earnings (descending)
        filteredData.sort((a, b) => b['Total Earnings'] - a['Total Earnings']);
        
        // Update filtered count
        document.getElementById('filteredCount').textContent = `Showing ${filteredData.length} out of ${tradersData.length} traders`;
        
        // Add rows to the table
        filteredData.forEach(trader => {
            const row = document.createElement('tr');
            
            const nameCell = document.createElement('td');
            nameCell.textContent = trader.Name;
            row.appendChild(nameCell);
            
            const locationCell = document.createElement('td');
            locationCell.textContent = trader.Location;
            row.appendChild(locationCell);
            
            const earningsCell = document.createElement('td');
            earningsCell.textContent = '$' + trader['Total Earnings'].toLocaleString();
            row.appendChild(earningsCell);
            
            const pagesCell = document.createElement('td');
            pagesCell.textContent = Array.isArray(trader.Pages) ? trader.Pages.join(', ') : trader.Pages;
            row.appendChild(pagesCell);
            
            tableBody.appendChild(row);
        });
        
        // Update charts
        updateCharts();
    }
    
    // Function to update the charts
    function updateCharts() {
        // Top 10 traders chart
        const top10Traders = filteredData.slice(0, 10);
        const tradersCtx = document.getElementById('tradersChart').getContext('2d');
        
        if (window.tradersChart) {
            window.tradersChart.destroy();
        }
        
        window.tradersChart = new Chart(tradersCtx, {
            type: 'bar',
            data: {
                labels: top10Traders.map(t => t.Name),
                datasets: [{
                    label: 'Earnings ($)',
                    data: top10Traders.map(t => t['Total Earnings']),
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
        
        // Countries chart
        const countriesData = {};
        filteredData.forEach(trader => {
            const country = trader.Country || 'Unknown';
            if (!countriesData[country]) {
                countriesData[country] = 0;
            }
            countriesData[country] += trader['Total Earnings'];
        });
        
        const countriesCtx = document.getElementById('countriesChart').getContext('2d');
        
        if (window.countriesChart) {
            window.countriesChart.destroy();
        }
        
        const sortedCountries = Object.entries(countriesData)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10);
        
        window.countriesChart = new Chart(countriesCtx, {
            type: 'pie',
            data: {
                labels: sortedCountries.map(c => c[0]),
                datasets: [{
                    data: sortedCountries.map(c => c[1]),
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.5)',
                        'rgba(54, 162, 235, 0.5)',
                        'rgba(255, 206, 86, 0.5)',
                        'rgba(75, 192, 192, 0.5)',
                        'rgba(153, 102, 255, 0.5)',
                        'rgba(255, 159, 64, 0.5)',
                        'rgba(199, 199, 199, 0.5)',
                        'rgba(83, 102, 255, 0.5)',
                        'rgba(40, 159, 64, 0.5)',
                        'rgba(210, 199, 199, 0.5)'
                    ],
                    borderColor: [
                        'rgba(255, 99, 132, 1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 206, 86, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)',
                        'rgba(199, 199, 199, 1)',
                        'rgba(83, 102, 255, 1)',
                        'rgba(40, 159, 64, 1)',
                        'rgba(210, 199, 199, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label || '';
                                const value = context.raw;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = Math.round((value / total) * 100);
                                return `${label}: $${value.toLocaleString()} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    // Apply filters function
    function applyFilters() {
        const countryFilter = document.getElementById('countryFilter').value;
        const minAmount = parseFloat(document.getElementById('minAmount').value) || 0;
        const searchName = document.getElementById('searchName').value.toLowerCase();
        
        filteredData = tradersData.filter(trader => {
            // Country filter
            if (countryFilter && trader.Country !== countryFilter) {
                return false;
            }
            
            // Minimum amount filter
            if (trader['Total Earnings'] < minAmount) {
                return false;
            }
            
            // Name search
            if (searchName && !trader.Name.toLowerCase().includes(searchName)) {
                return false;
            }
            
            return true;
        });
        
        updateTable();
    }
    
    // Event listeners
    document.getElementById('applyFilters').addEventListener('click', applyFilters);
    document.getElementById('resetFilters').addEventListener('click', function() {
        document.getElementById('countryFilter').value = '';
        document.getElementById('minAmount').value = '';
        document.getElementById('searchName').value = '';
        filteredData = [...tradersData];
        updateTable();
    });
    
    // Initialize the table and charts
    document.addEventListener('DOMContentLoaded', function() {
        updateTable();
    });
    """
    
    # Replace the placeholder with actual data
    js_code = js_code.replace('REPLACE_WITH_DATA_JSON', data_json)
    
    # Identify known problematic pages
    problematic_pages = [349]  # Add any known problematic pages here
    skipped_pages = [p for p in failed_pages if p in problematic_pages] if failed_pages else []
    actual_failed_pages = [p for p in failed_pages if p not in problematic_pages] if failed_pages else []
    
    # Add a section for skipped pages in the HTML
    skipped_pages_html = ""
    if skipped_pages:
        skipped_pages_html = f"""
        <div class="skipped-pages">
            <h2>Skipped Pages</h2>
            <p class="warning">The following {len(skipped_pages)} pages were skipped due to known issues:</p>
            <table class="table table-sm">
                <tr><th>Page Number</th><th>Reason</th></tr>
                {''.join([f'<tr><td>{page}</td><td>Known problematic page</td></tr>' for page in sorted(skipped_pages)])}
            </table>
        </div>
        """
    
    # Create the HTML content with the JavaScript properly enclosed
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{'Interim ' if is_interim else ''}Apex Trader Funding Payout Scraping Report</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333366; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .summary {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .success {{ color: green; }}
            .warning {{ color: orange; }}
            .error {{ color: red; }}
            .progress-bar-container {{ width: 100%; background-color: #f0f0f0; border-radius: 4px; margin: 10px 0; }}
            .progress-bar {{ height: 20px; background-color: #4CAF50; border-radius: 4px; text-align: center; color: white; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
            .data-preview {{ margin-top: 20px; }}
            .chart-container {{ margin-top: 30px; }}
            .filters {{ margin: 20px 0; padding: 15px; background-color: #f8f9fa; border-radius: 5px; }}
            .standalone-note {{ background-color: #e8f4f8; padding: 10px; border-radius: 5px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>{'Interim ' if is_interim else ''}Apex Trader Funding Payout Scraping Report</h1>
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>Report generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Status:</strong> {'In Progress' if is_interim else 'Completed'}</p>
                
                {f'<div class="progress-bar-container"><div class="progress-bar" style="width: {current_progress}%">{current_progress:.1f}%</div></div>' if is_interim else ''}
                
                <p><strong>Pages scraped:</strong> {successful_pages} / {total_pages} ({(successful_pages/total_pages*100):.1f}%)</p>
                <p><strong>Failed pages:</strong> {len(actual_failed_pages) if actual_failed_pages else 0} / {total_pages} ({(len(actual_failed_pages)/total_pages*100 if actual_failed_pages else 0):.1f}%)</p>
                <p><strong>Skipped pages:</strong> {len(skipped_pages) if skipped_pages else 0} / {total_pages} ({(len(skipped_pages)/total_pages*100 if skipped_pages else 0):.1f}%)</p>
                
                {'<p><em>Note: This is an interim report. Scraping is still in progress.</em></p>' if is_interim else ''}
                
                {f'<p><strong>Date range:</strong> {start_date.strftime("%Y-%m-%d") if start_date else "N/A"} to {end_date.strftime("%Y-%m-%d") if end_date else "N/A"}</p>' if start_date and end_date else '<p><strong>Date range:</strong> N/A</p>'}
            </div>
            
            {batch_size_html}
            
            {skipped_pages_html}
            
            {f'''
            <div class="failed-pages">
                <h2>Failed Pages</h2>
                {'<p class="success">No failed pages!</p>' if not actual_failed_pages or len(actual_failed_pages) == 0 else f'<p class="warning">The following {len(actual_failed_pages)} pages failed to scrape:</p>'}
                {'<table class="table table-sm"><tr><th>Page Number</th></tr>' + ''.join([f'<tr><td>{page}</td></tr>' for page in sorted(actual_failed_pages)]) + '</table>' if actual_failed_pages and len(actual_failed_pages) > 0 else ''}
            </div>
            ''' if (actual_failed_pages and len(actual_failed_pages) > 0) or not is_interim else ''}
            
            {'''
            <div class="data-files">
                <h2>Data Files</h2>
                <p>Data has been saved to the following files:</p>
                <ul>
                    <li>apex_payouts.csv - Raw payout data</li>
                    <li>aggregated_payouts.csv - Aggregated payout data by trader</li>
                </ul>
            </div>
            ''' if not embed_data else '''
            <div class="standalone-note">
                <h3>Standalone Report</h3>
                <p>This is a self-contained report with all data embedded. You can share this HTML file directly without needing any CSV files.</p>
            </div>
            '''}
            
            {f'''
            <div class="data-preview">
                <h2>Data Analysis</h2>
                
                <div class="filters">
                    <h3>Filters</h3>
                    <div class="row">
                        <div class="col-md-4">
                            <label for="countryFilter" class="form-label">Filter by Country:</label>
                            <select id="countryFilter" class="form-select">
                                <option value="">All Countries</option>
                                {country_options}
                            </select>
                        </div>
                        <div class="col-md-4">
                            <label for="minAmount" class="form-label">Minimum Earnings:</label>
                            <input type="number" id="minAmount" class="form-control" placeholder="Min amount">
                        </div>
                        <div class="col-md-4">
                            <label for="searchName" class="form-label">Search by Name:</label>
                            <input type="text" id="searchName" class="form-control" placeholder="Trader name">
                        </div>
                    </div>
                    <div class="row mt-3">
                        <div class="col-12">
                            <button id="applyFilters" class="btn btn-primary">Apply Filters</button>
                            <button id="resetFilters" class="btn btn-secondary">Reset</button>
                        </div>
                    </div>
                </div>
                
                <div class="row mt-4">
                    <div class="col-md-6">
                        <h3>Top Traders by Earnings</h3>
                        <canvas id="tradersChart"></canvas>
                    </div>
                    <div class="col-md-6">
                        <h3>Earnings by Country</h3>
                        <canvas id="countriesChart"></canvas>
                    </div>
                </div>
                
                <div class="mt-4">
                    <h3>Trader Data</h3>
                    <p id="filteredCount">Showing all {len(df)} traders</p>
                    <table id="tradersTable" class="table table-striped table-hover">
                        <thead>
                            <tr>
                                <th>Name</th>
                                <th>Location</th>
                                <th>Total Earnings</th>
                                <th>Pages</th>
                            </tr>
                        </thead>
                        <tbody id="tableBody">
                            <!-- Data will be populated by JavaScript -->
                        </tbody>
                    </table>
                </div>
            </div>
            
            <script>
            {js_code}
            </script>
            ''' if has_data and not df.empty else f'<div class="alert alert-info">No data available yet. Check back after more pages have been scraped.</div>'}
        </div>
    </body>
    </html>
    """
    
    # Write the HTML content to a file in the reports directory
    output_file = 'reports/payout_report_standalone.html' if embed_data else 'reports/payout_report.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Report generated as '{output_file}'")
    
    return output_file

if __name__ == "__main__":
    generate_html_report() 