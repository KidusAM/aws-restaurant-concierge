"""
This sample demonstrates an implementation of the Lex Code Hook Interface
in order to serve a sample bot which manages orders for flowers.
Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
as part of the 'OrderFlowers' template.

For instructions on how to set up and test this bot, as well as additional samples,
visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """
dynamodb = boto3.client('dynamodb')


def close(session_attributes, fulfillment_state, message):
    response = {
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

""" --- Functions that control the bot's behavior --- """

def greet_customer(intent_request):
    """
    Performs dialog management and fulfillment for greeting customers.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """

    # Order the flowers, and rely on the goodbye message of the bot to define the message to the end user.
    # In a real bot, this would likely involve a call to a backend service.
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hello, how can I help?'})
                 

def make_recommendation(intent_request):
    cur_item = intent_request['currentIntent']['slots']
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/308030062589/SuggestionsQueue'
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = json.dumps(cur_item),
    )
    
    # save the preferences of the user in dynamodb
    prefs = {
        'user_id' : {'S' : intent_request['userId']}, 
        'location_pref' : {'S' : cur_item['Location']},
        'cuisine_pref' : {'S' : cur_item['Cuisine']}
    }
    dynamodb.put_item(
        TableName='user-preferences',
        Item=prefs)
    
    return close('', 'Fulfilled', {'contentType': 'PlainText', 'content': 'Got It. I will send you my recommendation soon.'})
""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']
    user_id = intent_request['userId']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetIntent':
        return greet_customer(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        slots = intent_request['currentIntent']['slots']
        null_slots = len([k for k,v in slots.items() if v is None])
        if null_slots == 0:
            return make_recommendation(intent_request)
        
        # see if the user already has location/cuisine preferences from their last interaction
        pref = dynamodb.get_item(
            TableName = 'user-preferences',
            Key = {'user_id' : {'S' : user_id}})
        if 'Item' in pref:
            location_pref = pref['Item']['location_pref']['S']
            cuisine_pref = pref['Item']['cuisine_pref']['S']
            slots['Location'] = location_pref
            slots['Cuisine'] = cuisine_pref
        
        
        next_slot = ''
        for cur_slot in ['Location', 'Cuisine', 'DiningDate', 'DiningTime', 'NumPeople', 'EmailAddress']:
            if slots[cur_slot] is None:
                next_slot = cur_slot
                break
        return {
            'dialogAction' : 
                {
                    'type' : 'ElicitSlot',
                    'intentName' : intent_name,
                    'slotToElicit' : next_slot,
                    'slots' : slots
                }
        }

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)

