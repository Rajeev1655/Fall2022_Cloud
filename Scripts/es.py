import json
import boto3
import requests
from boto3.dynamodb.conditions import Key
#from botocore.vendored import requests

dynamodb = boto3.resource('dynamodb', region_name='us-east-1',aws_access_key_id='acesskey',aws_secret_access_key='secretkey')
table = dynamodb.Table('yelp-restaurant')

resp = table.scan()
i = 1
url = 'https://search-yelprestaurant-6gxrqrb7ihcrtht76iupswk3xu.us-east-1.es.amazonaws.com'
path = '/restaurantbot/_doc'

service = 'es'

url += path

headers = {"Content-Type": "application/json"}
while True:
    print(len(resp['Items']))
    for item in resp['Items']:
        body = {"RestaurantID": item['business_id'], "Cuisine": item['cuisine']}
        print(body)
        r = requests.post(url, auth = ('username', 'pass'), data=json.dumps(body).encode("utf-8"), headers=headers)
        print(r)
        i += 1
    if 'LastEvaluatedKey' in resp:
        resp = table.scan(
        ExclusiveStartKey=resp['LastEvaluatedKey']
        )
    else:
        break
print(i)
