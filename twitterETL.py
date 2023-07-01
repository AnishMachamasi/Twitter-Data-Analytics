import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "Q7199JH4Q31WEOpZM17U8cKq7" 
    access_secret = "eGQpQiCebdfC0jdxhQ87Wt6kNHIeMOSelGgz4VsUaziTkVE2gq" 
    consumer_key = "320105897—XZDsbECwbTQAv6oxKDFRKO5f4ig4MDs1X11FvRud"
    consumer_secret = "BEYGQG6FJ-t8hsxTtB5x6muHGw9U8xj tP1BmitpfEJm5ce"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name, # storing the user screen name who tweeted
                        'text' : text,  # storing the text field
                        'favorite_count' : tweet.favorite_count, # likes
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('tweets.csv')
