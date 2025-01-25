import pandas as pd
import json

def generate_html_report(csv_file='aggregated_payouts.csv', successful_pages=None, total_pages=None, start_date=None, end_date=None, failed_pages=None):
    # Read the aggregated CSV file
    df = pd.read_csv(csv_file)
    
    # Extract country from Location (assuming format "State, Country" or just "Country")
    df['Country'] = df['Location'].apply(lambda x: x.split(',')[-1].strip())
    
    # Convert the DataFrame to JSON for JavaScript
    data_json = df.to_json(orient='records')
    
    # Get unique countries for dropdown
    countries = sorted(df['Country'].unique())
    
    # Generate country options HTML
    country_options = '\n'.join([f'<option value="{country}">{country}</option>' for country in countries])
    
    # Format date range string
    date_range = ""
    if start_date and end_date:
        date_range = f"Report represents data from {end_date.strftime('%B %d, %Y')} to {start_date.strftime('%B %d, %Y')}"
    
    # Format pages info
    pages_info = ""
    if successful_pages is not None and total_pages is not None:
        pages_info = f"Successfully scraped {successful_pages} out of {total_pages} pages"
        if failed_pages and len(failed_pages) > 0:
            pages_info += f"<br>Failed to scrape pages: {', '.join(map(str, failed_pages))}"
    
    # Create the HTML content directly
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Apex Trader Funding Payouts Report</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
        <style>
            .container {{ margin-top: 30px; }}
            #payoutTable {{ margin-top: 20px; }}
            .total-stats {{
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 5px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="mb-4">Apex Trader Funding Payouts Report</h1>
            
            <div class="alert alert-info mb-4" style="display: block !important;">
                <p class="mb-1">{date_range}</p>
                <p class="mb-0">{pages_info}</p>
            </div>
            
            <div class="row">
                <div class="col-md-4">
                    <select id="countryFilter" class="form-select">
                        <option value="all">All Countries</option>
                        {country_options}
                    </select>
                </div>
            </div>

            <div class="total-stats mt-4">
                <div class="row">
                    <div class="col-md-4">
                        <h5>Total Traders: <span id="totalTraders">0</span></h5>
                    </div>
                    <div class="col-md-4">
                        <h5>Total Payouts: $<span id="totalPayouts">0</span></h5>
                    </div>
                    <div class="col-md-4">
                        <h5>Average Payout: $<span id="avgPayout">0</span></h5>
                    </div>
                </div>
            </div>

            <table id="payoutTable" class="table table-striped">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Location</th>
                        <th>Total Earnings</th>
                        <th>Pages</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                </tbody>
            </table>
        </div>

        <script>
            // Store the data
            const data = {data_json};
            
            // Format number as currency
            function formatCurrency(number) {{
                return number.toLocaleString('en-US', {{
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2
                }});
            }}
            
            // Update table based on selected country
            function updateTable(country) {{
                const tableBody = document.getElementById('tableBody');
                tableBody.innerHTML = '';
                
                let filteredData = data;
                if (country !== 'all') {{
                    filteredData = data.filter(item => item.Country === country);
                }}
                
                // Sort by Total Earnings (descending)
                filteredData.sort((a, b) => b['Total Earnings'] - a['Total Earnings']);
                
                // Update stats
                document.getElementById('totalTraders').textContent = filteredData.length;
                const totalPayouts = filteredData.reduce((sum, item) => sum + item['Total Earnings'], 0);
                document.getElementById('totalPayouts').textContent = formatCurrency(totalPayouts);
                document.getElementById('avgPayout').textContent = formatCurrency(totalPayouts / filteredData.length || 0);
                
                // Populate table
                filteredData.forEach(item => {{
                    const row = document.createElement('tr');
                    row.innerHTML = '<td>' + item.Name + '</td>' +
                        '<td>' + item.Location + '</td>' +
                        '<td>$' + formatCurrency(item['Total Earnings']) + '</td>' +
                        '<td>' + item.Pages + '</td>';
                    tableBody.appendChild(row);
                }});
            }}
            
            // Initialize the table and add event listener
            document.addEventListener('DOMContentLoaded', function() {{
                updateTable('all');
                document.getElementById('countryFilter').addEventListener('change', function(e) {{
                    updateTable(e.target.value);
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    # Write to file
    with open('payout_report.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("Report generated as 'payout_report.html'")

if __name__ == "__main__":
    generate_html_report() 