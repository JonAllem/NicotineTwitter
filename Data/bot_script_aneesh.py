import pandas as pd
import numpy as np
import tqdm as tqdm
import botometer
import pickle

start = 0
end = 100

print("Starting at: " + str(start))
print("Ending at: " + str(end))
print("Total: " + str(end - start))

unique_users = pd.read_csv(r"D:\users\users.csv")
unique_users = unique_users['0']

rapidapi_key = "RAPIDAPI_KEY"
twitter_app_auth = {
    'consumer_key': 'CONSUMER_KEY',
    'consumer_secret': 'CONSUMER_SECRET'
  }
api_url = 'https://botometer-pro.p.mashape.com'
bom = botometer.Botometer(botometer_api_url=api_url,wait_on_ratelimit=True,
                          rapidapi_key=rapidapi_key,
                          **twitter_app_auth)

account_scores = {}

for screenname, result in  tqdm.tqdm(bom.check_accounts_in(unique_users[start:end])):
    if 'error' in result:
        account_scores[screenname] = None
    else:
        account_scores[screenname] = result

with open('account_scores_' + str(start) + '_' + str(end) + '.pickle', 'wb') as file:
    pickle.dump(account_scores, file)