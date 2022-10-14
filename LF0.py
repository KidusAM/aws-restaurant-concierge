import json
import boto3
import uuid
import time

def lambda_handler(event, context):
    text = event["messages"][0]['unstructured']['text']
    client = boto3.client('lex-runtime')
    your_id = event['messages'][0]['unstructured'].get('id', "")
    # If the user is a new user, assign them a new ID
    if not your_id or len(your_id) == 0:
        your_id = str(uuid.uuid1())
    
    bot_response = client.post_text(
        botName = 'RestaurantBot',
        botAlias = 'test',
        userId = your_id,
        inputText= text
        )

    return {
        "messages": [{
                "type": "unstructured",
                "unstructured": {
                    "id": your_id,
                    "text": bot_response['message'],
                    "timestamp": str(time.time())
                }
            }
        ]
    }

