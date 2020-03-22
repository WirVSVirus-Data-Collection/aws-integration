# Bulk daily report load for Johns Hopkins data in https://github.com/CSSEGISandData

from urllib.request import urlopen
import boto3

# Create an S3 client
s3 = boto3.client('s3')
bucket_name = 'wirvsvirus-data-lake-landing-zone'
filename = 'landkreise.json'

def lambda_handler(event, context):
    url = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    
    print("Retrieving URL " + url)
    
    with urlopen(url) as response:
        respjson = response.read()
        print('Retrieve OK, bucketing...')
        print(respjson)
        s3response = s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=respjson
        )
        return s3response
    return response

lambda_handler(None, None)
