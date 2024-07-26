import apache_beam as beam
import logging
from urllib.request import urlopen
from apache_beam.options.pipeline_options import PipelineOptions
from writer import WriteToMySQL
from parse_csv import ParseCSV

# GitHub repository raw file URL
input_file_url = 'https://media.githubusercontent.com/media/Wells-Fargo-Bootcamp/Customer_segmentation/main/Mall_Customers.csv'

# Fetches the CSV data from the given URL
def fetch_csv(url):
    try:
        response = urlopen(url)
        if response.status != 200:
            logging.error(f"Failed to fetch {url}: {response.status}")
        return response.read().decode('utf-8')
    except Exception as err:
        logging.error(f"Error fetching URL {url}: {err}")
        return None 
                
def run_pipeline():
    
    # Set the DirectRunner option
    pipeline_options = PipelineOptions(runner='DirectRunner', streaming=False)   
   
    with beam.Pipeline(options=pipeline_options) as pipeline:

        # Read from GitHub raw file URL
        data = (
            pipeline
            | 'Read CSV from GitHub' >> beam.Create([input_file_url])
            | 'Fetch CSV Data' >> beam.Map(fetch_csv)
        )
        
        # Parse CSV data
        parsed_data = data | 'Parse CSV Data' >> beam.ParDo(ParseCSV()).with_outputs('malformed', main='valid')

        # Write valid data to MySQL
        parsed_data.valid | 'Write to MySQL' >> beam.ParDo(WriteToMySQL(
            host='34.30.157.44',
            database='customer_segmentation',
            user='root',
            password='T%%ZI<#e7qh2=8HH',
            batch_size=100
        ))

        # Log malformed data for debugging
        parsed_data.malformed | 'Log Malformed Rows' >> beam.Map(lambda row: logging.warning(f"Malformed row logged: {row}"))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
