#installing the required packages 
import os
import praw
import sys
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv


#We have created a function to retrieve reddit token using client ID, secret, userName, and password
def getRedditToken(clientId, secret, userName , password):
    try:
        # Setting up HTTP basic authentication
        auth = requests.auth.HTTPBasicAuth(clientId, secret)

        # Prepare data for token retrieval
        data = {'grant_type': 'password', 'username': userName, 'password': password}

        #setting up the headers 
        headers = {'User-Agent': 'Data Streaming Application Assignment 3'}

        #Here, we are Defining the endpoint for token retrieval
        endpoint = 'https://www.reddit.com/api/v1/access_token'

        #Here, we are Sending a POST request to retrieve the token
        response = requests.post(endpoint, headers=headers, data=data, auth=auth, timeout=20)

        #Here we are checking if the request was successful
        if response.status_code != 200:
            return None
        
        # Returning the access token
        return response.json()["access_token"]
    
    except Exception as login_exception:
    
        # Handle login exceptions
        print("Login Exception while retrieving the token : " + login_exception)
        return None

#Sending Reddit data to a Kafka topic 
def sendRedditDataToKafka(data,kafkaTopic, bootStrapServer):

    # Creating a Kafka producer instance
    producer = KafkaProducer(bootstrap_servers=[bootStrapServer])

    # Sending the data to the specified Kafka topic
    producer.send(kafkaTopic, str.encode(data))

    # Flushing any remaining messages in the buffer to ensure they are sent
    producer.flush()

    # Closing the producer to free up resources
    producer.close()


# We have created a function to process comments from a Reddit subreddit and sending them to a Kafka topic. we have used praw for doing this .

def processRedditComments(clientId, secret, userName, password, bootstrapServer, kafkaTopic):

    # Here, we are Authenticating with Reddit using PRAW. Reddit class provides access to Reddit's API.

    reddit = praw.Reddit(client_id = clientId,
                        client_secret= secret,
                        username = userName,
                        user_agent = 'Reddit Data Streaming Application using Spark for Assignment 3',
                        password = password)

    # Here, we are choosing a subreddit called AskScienceFiction for our Assignment - 3

    subreddit = reddit.subreddit("AskReddit")

    # We are Streaming comments from the chosen subreddit ( Here : it's AskScienceFiction )

    for subRedditComment in subreddit.stream.comments(skip_existing=True):

        # Extract the body of the comment
        subRedditCommentBody = subRedditComment.body

        # Printing the subreddit comment body

        print(subRedditCommentBody)

        # Sending the comment data to Kafka using the function we have created earlier.

        sendRedditDataToKafka(subRedditCommentBody, kafkaTopic, bootstrapServer)

if __name__ == '__main__':
    # Extracting the command line arguments
    kafkaTopic = sys.argv[1]
    bootstrapServer = sys.argv[2]

    # Loading environment variables
    load_dotenv()

    #Passing the Reddit credentials generated while creating the script on https://www.reddit.com/prefs/apps/
    userName = "swarna_madhuri"
    password = "Bigd@t@123@"
    clientId = "3Wd1fWFD_RmZWvl8wG80KQ"
    secret = "A8spE2kip82RNlO7psKE_se152Y5wA"

    # Retrieving the Reddit token
    token = getRedditToken(clientId, secret, userName, password)

    # Processing Reddit comments if token is retrieved successfully
    if token:
        processRedditComments(clientId, secret, userName, password, bootstrapServer, kafkaTopic)
    else:
        print("Failed to retrieve the token.")
        sys.exit(1)
