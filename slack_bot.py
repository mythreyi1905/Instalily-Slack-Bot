
import slack
import os
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, request, Response
from elasticsearch import Elasticsearch
import re
import json
from slack_sdk.errors import SlackApiError
from elasticsearch.exceptions import RequestError
from slackeventsapi import SlackEventAdapter

env_path=Path('.')/'.env'
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
print(os.environ['SIGNING_SECRET'])
slack_event_adapter = SlackEventAdapter(os.environ['SIGNING_SECRET'],'/slack/events',app)

client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
BOT_ID = client.api_call("auth.test")['user_id']
#client.chat_postMessage(channel='#test',text="Hello World")


# create Elasticsearch instance
es = Elasticsearch(
    ['https://localhost:9200'],
    basic_auth=('elastic', 'ZV3DIP4R6q2eNh6BrLJV'),
    verify_certs=False
)

index_name = 'whiskey'


# mapping for index
mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "price": {"type": "float"},
            "rating": {"type": "float"},
            "about": {"type": "text"}
        }
    }
}

# create index with mapping
try:
    es.indices.create(index=index_name, body=mapping)
except RequestError as e:
    print(e)

# Read data from JSON file
with open('whiskey_data.json') as f:
    data = json.load(f)


# bulk index data
for doc in data:
    if doc['rating'] is not None:
        doc['rating'] = float(doc['rating'].split('(')[0])
    
    if doc['price'] is not None:
        doc['price'] = float(doc['price'].replace('£', '').replace(',', ''))
    es.index(index=index_name, body=doc)






# function to handle incoming Slack messages
@slack_event_adapter.on('message')
def message(payload):
    # extract information about the incoming message
    event = payload.get('event', {})
    channel_id = event.get('channel')
    user_id = event.get('user')
    text = event.get('text')

    # check if the bot sent the message
    if BOT_ID != user_id:
        # extract the search term and field from the message text
        match = re.search(r'"(\w+)"\s*:\s*"([^"]+)"', text)
        if match:
            field = match.group(1)
            search_term = match.group(2)
            # create a list of fields to search in
            fields = ["name", "price", "rating", "about"]
            # check if the specified field is valid
            if field not in fields:
                message = f"Invalid field '{field}'. Valid fields are: {', '.join(fields)}"
            else:
                # search for documents in the Elasticsearch index that match the search term and field
                res = es.search(index=index_name, body={"query": {"multi_match": {"query": search_term, "fields": [field]}}})
                # extract the search results from the Elasticsearch response
                hits = res.get('hits', {}).get('hits', [])
                # format the search results as a text message to send back to the Slack channel
                if hits:
                    message = f"Here are the results for '{search_term}' in the '{field}' field:\n\n"
                    for hit in hits:
                        message += f"Name: {hit['_source']['name']}\nPrice: {hit['_source']['price']}\nRating: {hit['_source']['rating']}\nAbout: {hit['_source']['about']}\n\n"
                else:
                    message = f"No results found for '{search_term}' in the '{field}' field"
            # post the message back to the Slack channel
            try:
                response = client.chat_postMessage(channel=channel_id, text=message)
            except SlackApiError as e:
                print(f"Error posting message: {e}")

@app.route('/message-count')
def message_count():
    return Response(), 200



@app.route('/')
def index():
    return '<h1>Hello, world!</h1>'
    
if __name__ == "__main__":
    app.run(debug=True, host='localhost',port=3000)

















