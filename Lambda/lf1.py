import boto3
import json
import logging
import time
import re

client = boto3.client('lex-runtime')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DEFAULT_TIMEZONE = "America/New_York"
VALID_CUISINES = ['italian', 'chinese', 'indian', 'american', 'mexican', 'spanish', 'greek', 'latin', 'Persian', "Korean", "South Indian"]
PEOPLE_LIMIT = [0, 10] #[min, max]

def lambda_handler(event, context):
    #os.environ['TZ'] = 'America/New_York'
    #time.tzset(DEFAULT_TIMEZONE)
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return handle_event(event)

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']
    
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def handle_event(event):
    
    logger.info(
        'dispatch userId={}, intentName={}'.format(event['userId'], event['currentIntent']['name']))
    
    intent_type = event['currentIntent']['name']
    if intent_type == 'GreetingIntent':
        return handle_greeting(event)
    elif intent_type == 'DiningSuggestionIntent':
        return diningSuggestionEvent(event)
    elif intent_type == 'ThankYouIntent':
        return handle_thank_you_event(event)

    raise Exception("We don't support the intent {}".format(intent_type))
    
def handle_greeting(event):
    logger.debug("Parsing a greeting event {}".format(event))
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I assist you today?'}
        }
    }
    
def handle_thank_you_event(event):
    logger.debug("Parsing a thank you event {}".format(event))
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome, Have a nice day!'}
        }
    }

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def validateDiningSuggestion(location, cuisine, noOfPeople, date, time, phone, email):
    
    if cuisine is not None and cuisine.lower() not in VALID_CUISINES:
        return validation_response(False,
                                       'cuisine',
                                       'Cuisine not available. Please try another.')

    if noOfPeople is not None:
        noOfPeople = int(noOfPeople)
        if noOfPeople > PEOPLE_LIMIT[1] or noOfPeople < PEOPLE_LIMIT[0]:
            return validation_response(False,
                                           'people',
                                           'Due to covid, we are not allowing more than 10 people.')
    if phone is not None:
        regex= "\w{3}\w{3}\w{4}"
        if not re.search(regex, phone):
            logger.debug("The phone number {} is not valid".format(phone))
            return validation_response(False,
                                            'phone',
                                            'Please enter a valid phone number.')     
    if email is not None:
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if not re.fullmatch(regex, email):
            logger.debug("The email {} is not valid".format(email))
            return validation_response(False,
                                            'email',
                                            'Please enter a valid email address.')  
            
                                           
    return validation_response(True, None, None)
    
           
def validation_response(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
def diningSuggestionEvent(event):
    logger.info("Parsing a dining event - {}".format(event))
    location = get_slots(event)["Location"]
    cuisine = get_slots(event)["Cuisine"]
    noOfPeople = get_slots(event)["PeopleCount"]
    date = get_slots(event)["Date"]
    time = get_slots(event)["Time"]
    source = event['invocationSource']
    phone = get_slots(event)["Mobile"]
    email= get_slots(event)["Email"]
    slots = [location, cuisine, date, noOfPeople, time, phone,email]
    if source == 'DialogCodeHook':
        slots = get_slots(event)

        validateResult = validateDiningSuggestion(location, cuisine, noOfPeople, date, time, phone, email)

        if not validateResult['isValid']:
            logger.info("invalid response")
            slots[validateResult['violatedSlot']] = None
            return elicit_slot(event['sessionAttributes'],
                               event['currentIntent']['name'],
                               slots,
                               validateResult['violatedSlot'],
                               validateResult['message'])
        logger.info("Pushing to the queue after clearing all the validations")
       # queue = sqs.get_queue_by_name(QueueName='Queue1')
        sqs= boto3.client('sqs')
        push_to_sqs = True
        msg = {"cuisine": cuisine, "phone": phone, "city": location, "noOfPeople":noOfPeople, "date":date, "time":time, "email":email
        }
        for k,v in msg.items():
            if v == None:
                push_to_sqs = False
                break
        
        logger.info("Pushing to sqs: {}".format(msg))
        #response = queue.send_message(MessageBody=json.dumps(msg))
        if push_to_sqs:
            response=sqs.send_message( QueueUrl='https://sqs.us-east-1.amazonaws.com/143326423142/q1',
            MessageBody=json.dumps(msg)   
            )
        if event[
            'sessionAttributes'] is not None:
            logger.info("invalid sessionAttributes")
            outputAttributes = event['sessionAttributes']
        else:
            outputAttributes = {}
        logger.info("returning delegate")
        return delegate(outputAttributes, get_slots(event))

   

    return close(event['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! You will recieve suggestion shortly'})
                  
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response