# Bulk daily report load for Johns Hopkins data in https://github.com/CSSEGISandData

from datetime import date, timedelta
from urllib.parse import quote_plus
from urllib.request import urlopen
import boto3

# Create an S3 client
s3 = boto3.client('s3')
bucket_name = 'wirvsvirus-test-bucket'


def lambda_handler(event, context):
    
    today = date.today()
    startDate = date(2020, 1, 22)
    delta = timedelta(days=1)

    while startDate < today:

        currentDate = startDate.strftime("%m-%d-%Y")
        filename = currentDate + ".csv"

        downloadPath = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/%s.csv' % quote_plus(currentDate)

        with urlopen(downloadPath) as response:
            csvResponse = response.read()
            
        response = s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=csvResponse
        )
        
        startDate += delta

