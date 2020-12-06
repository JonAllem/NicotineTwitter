import tweepy
import pandas as pd
import sys
import time
import os

'''
Script to fetch like and retweet counts from a given list of tweet ids
'''

file_name = "tweet-ids.csv"
typ = file_name.split(".")[0]
start_index = 0
end_index = 0



df = pd.read_csv(file_name)

df.head()

twitter_app_auth = {'consumer_key': '',
                    'consumer_secret': '',
                    'access_token': '',
                    'access_token_secret': ''
                   }

auth = tweepy.OAuthHandler(twitter_app_auth['consumer_key'], twitter_app_auth['consumer_secret'])
auth.set_access_token(twitter_app_auth['access_token'], twitter_app_auth['access_token_secret'])
api = tweepy.API(auth)

#df_subset = df.iloc[start_index: end_index]

end_index = len(df)

df_subset = df

c = 0
op = []
for index, row in df_subset.iterrows():      
    tweet_id = row['id']
    if(index%20==0):
        print("fetching index " + str(index))
    if(index%1000 == 0 or index == len(df_subset)-1):
        if(op):
            print("writing on file")
            df = pd.DataFrame(op,columns =["index","fav_count","retweet_count"])
            df.to_csv(typ + "-"+ str(op[0][0]) + "-" + str(op[-1][0]) + ".csv",index = False)
            op = []
    try:
        res = api.get_status(tweet_id, trim_user=True, include_my_retweet=False, include_entities=False, include_ext_alt_text=False)
        retweet_count = res.retweet_count
        favorite_count = res.favorite_count
        op.append([index,favorite_count,retweet_count])
        
    except tweepy.RateLimitError:
        print("Sleeping for 15 min")
        time.sleep(15 * 60)
    except:
        op.append([index,"NA","NA"])
        continue
if(op):
    print("writing on file")
    df = pd.DataFrame(op,columns =["index","fav_count","retweet_count"])
    df.to_csv(typ + "-"+ str(op[0][0]) + "-" + str(op[-1][0])+'extra' + ".csv",index = False)
    op = []
print("fetched all from " + str(start_index) + " to " + str(end_index))






