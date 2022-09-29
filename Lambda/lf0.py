import json

def lambda_handler(event, context):
    botMessage="Application under development. Search functionality will be implemented in Assignment 2"
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    return {
        'statusCode': 200,
        'messages': botResponse
    }
