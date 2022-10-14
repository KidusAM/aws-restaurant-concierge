import json
import logging
import boto3
import requests
from requests_aws4auth import AWS4Auth
import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def choose_restaurant(cuisine):
    """ Searches the opensearch index for the cuisine and returns the id of 3
    random restaurants. """
    os_domain = 'https://search-restaurantscuisinedomain-3lysmwsbjfmsnmnc754dmm5x6e.us-east-1.es.amazonaws.com'
    
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    response = requests.get(os_domain + '/_search', auth=awsauth, params = {'q' : cuisine, 'size' : '1000'})
    
    hits = json.loads(response.text)['hits']['hits']
    chosen = set()
    while len(chosen) < 3 :
        cur_chosen = hits[random.randint(0, len(hits) - 1)]
        chosen.add(cur_chosen['_source']['id'])
    return chosen

def handle_message(message):
    ses = boto3.client('ses')

    slots = json.loads(message['Body'])
    
    chosen_cuisine, chosen_date, chosen_time, num_people, dest_email = slots['Cuisine'], slots['DiningDate'], slots['DiningTime'], slots['NumPeople'], slots['EmailAddress']

    
    chosen_ids = choose_restaurant(chosen_cuisine)
    
    chosen_restaurants = []
    for res_id in chosen_ids:
        dynamodb = boto3.client('dynamodb')
        
        response = dynamodb.get_item(
            TableName='yelp-restaurants', Key={'id' : {'S' : res_id}})
        
        res =  response['Item']
        name = res['name']['S']
        address = res['address']['L'][0]['S']
        chosen_restaurants.append((name, address))
    
    # Compose the email text
    email_text = """Hello! Here are my recommendations for {num_people} people for {cuisine} food for {chosen_date} {chosen_time}:\n""".format(
        cuisine=chosen_cuisine, num_people = num_people, chosen_date = chosen_date, chosen_time = chosen_time)
    for i in range(len(chosen_restaurants)):
        name, loc = chosen_restaurants[i]
        email_text += str(i + 1) + ": " + name + ' located at ' + loc + '\n'
    email_text += 'Enjoy!'
    
    response = ses.send_email(Source='km3533@columbia.edu', Destination= {'ToAddresses' : [dest_email]}, 
    Message={
        'Subject' : {'Data': 'Restaurant recommendation'},
        'Body' : {
            'Text' : {
                'Data' : email_text,
            }
        }})
        

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/308030062589/SuggestionsQueue'

    response = sqs.receive_message(QueueUrl = queue_url, MaxNumberOfMessages=10)
    for message in response['Messages']:
        receipt_handle = message['ReceiptHandle']
        handle_message(message)
        sqs.delete_message(QueueUrl = queue_url,receiptHandle = receipt_handle)
        
    logger.debug("I got a queue message", message)
    # TODO implement
    return {
        'statusCode': 200,
        'body': message
    }

