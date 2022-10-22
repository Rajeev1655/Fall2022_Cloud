import boto3
import requests
import datetime
import json
from decimal import Decimal

categories = ['indian', 'chinese', 'mexican', 'italian', 'french', 'japanese', 'american', 'continental', 'mediterrian']
citites = ['manhattan', 'albany', 'brooklyn', 'yonkers', 'syracuse', 'buffalo', 'rochester', 'bronx', 'queens', 'staten island']
# categories = ['indian']
# citites = ['manhattan']
url = 'https://api.yelp.com/v3/businesses/search'
headers = {'Authorization':'Bearer API_TOKEN'}

dynamodb = boto3.resource('dynamodb', region_name='us-east-1',aws_access_key_id='acesskey',aws_secret_access_key='secretkey')
table = dynamodb.Table('yelp-restaurant')

def writeInBatch(responses,cuisine):
    resp = responses['businesses']
    print(resp)
    with table.batch_writer() as batch:
        for i in range(len(resp)):
            batch.put_item(
                Item={
                    'business_id': resp[i]['id'],
                    'name': resp[i]['name'],
                    'address': resp[i]['location']['display_address'],
                    'latitude': Decimal(str(resp[i]['coordinates']['latitude'])),
                    'longitude': Decimal(str(resp[i]['coordinates']['longitude'])),
                    'num_of_reviews':resp[i]['review_count'],
                    'rating': Decimal(str(resp[i]['rating'])),
                    'zip_code' : resp[i]['location']['zip_code'],
                    'insertedAtTimestamp': str(datetime.datetime.now()),
                    'cuisine': str(cuisine)
                }
            )


for i in categories:
    offset=0
    while offset < 1000:
        for j in citites:
            url_params = url_params = {
            'categories': i,
            'location': j,
            'limit': 2
            }
            response = requests.get(url, headers = headers, params = url_params)
            jsonResponse = json.loads(response.content.decode("utf-8"))
            writeInBatch(jsonResponse,i)
            print(response.content)
        offset+= 50
# Get the service resource.
# dynamodb = boto3.resource('dynamodb')

# # Instantiate a table resource object without actually
# # creating a DynamoDB table. Note that the attributes of this table
# # are lazy-loaded: a request is not made nor are the attribute
# # values populated until the attributes
# # on the table resource are accessed or its load() method is called.
# table = dynamodb.Table('users')

# # Print out some data about the table.
# # This will cause a request to be made to DynamoDB and its attribute
# # values will be set based on the response.
# print(table.creation_date_time)
