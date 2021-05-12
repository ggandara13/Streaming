import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json



# Set up your credentials

consumer_key='CCZb6sSyLlcheo5bMKdGZdfWb'
consumer_secret='a7HCRWSzHKJW8nvPNE5yF0akOLJj0DnWeaC47CvbmX9k9VhPW7'
access_token ='27010823-HfutVHuX2yJoiRt50yNS1cgb7HaA67wfM1MXmAY0f'
access_secret='t3xnYVJpCxo0uPX7myiqxEfd2MS6aMD1JJha7CgMtkKDv'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)
api = tweepy.API(auth, wait_on_rate_limit=True)


user = api.me()
print (user.name)


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads( data )
            print( msg['text'].encode('utf-8') )
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print("Error GG - > ", status)
        return True
    

def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['Monica Garcia', 'Rocio Monasterio', 'Edmundo Bal', \
                                 'Ayuso', 'Angel Gabilondo', 'Pablo Iglesias']) # this is the topic we are interested in
    
    

if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "127.0.0.1"     # local machine address
    port = 5555                 # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)                 #  waiting for client connection.
    c, addr = new_skt.accept()        # Establish connection with client. it returns first a socket object,c

    print("Received request from: " + str(addr))
    # and after accepting the connection, we can send the tweets through the socket
    send_tweets(c)