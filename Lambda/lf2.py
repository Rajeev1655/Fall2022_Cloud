import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
import requests
# from requests_aws4auth import AWS4Auth
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
def getSQSMsg():
    client1 = boto3.client('sqs')
    queues = client1.list_queues(QueueNamePrefix='q1')
    print(queues)
    queue_url = queues['QueueUrls'][0]
        # get the response from the queue
    response = client1.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
        'All'
    ],
    MaxNumberOfMessages=10,
    MessageAttributeNames=[
            'All'
    ],
    VisibilityTimeout=30,
    WaitTimeSeconds=0
    )
    
    print(response)
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    client1.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('********Received and deleted message: %s' % response)
    return json.loads(message["Body"])
def lambda_handler(event, context):
    """
        Query SQS to get the messages
        Store the relevant info, and pass it to the Elastic Search
    """
    message = getSQSMsg() #data will be a json object
    print("here is the message")
    print(message)
    if message is None:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    cuisine = message.get("cuisine","")
    phoneNumber = message.get("phone","")
    location=message.get("city","")
    numOfPeople=message.get("noOfPeople","")
    date=message.get("date","")
    time=message.get("time","")
    email= message.get("email","")
    phoneNumber = "+1" + phoneNumber
    if not cuisine or not phoneNumber:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    print(cuisine,email)
    region = 'us-east-1' # For example, us-west-1
    service = 'es'
    credentials = boto3.Session().get_credentials()
    # awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    url = 'https://search-yelprestaurant-6gxrqrb7ihcrtht76iupswk3xu.us-east-1.es.amazonaws.com/restaurantbot/_search'
    # url = host + '/_search'
    print(url)
    query = {
          "size": 5,
          "query": {
              "multi_match": {
                  "query": str(cuisine),
                  "fields": ["Cuisine"]
              }
          }
        }

    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    r = requests.get(url, auth=('username', 'password'), headers=headers, data=json.dumps(query))
    data = json.loads(r.content.decode('utf-8'))
    esData=[]
    try:
        esData = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    # extract bID from AWS ES
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["RestaurantID"])
    print(ids)
    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, for {diningDate} at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurant')
    itr = 1
    for id in ids:
        if itr == 6:
            break
        response = table.scan(FilterExpression=Attr('business_id').eq(id))
        item = response['Items'][0]
        if response is None:
            continue
        print(response)
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]
        address = item["address"]
        print(name, address,"inLF")
        restaurantMsg += name +', located at ' + " ".join(address) +'. \n'
        messageToSend += restaurantMsg
        itr += 1
    messageToSend += "Enjoy your meal!!"
    sendEmail(messageToSend, email)
    print(messageToSend)
    print(message)


def sendEmail (sendMessage, toEmail):
    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"
    ses_client.send_email(
        Destination={
            "ToAddresses": [
                toEmail,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": sendMessage,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Dining Suggestions",
            },
        },
        Source="rajeev5499.koppur@gmail.com",
    )