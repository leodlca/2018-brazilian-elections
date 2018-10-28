import twitter, re, threading, datetime, glob, json, random, pandas as pd

class TwitterMiner():
    request_limit = 20
    api = False
    data = []
    
    twitter_keys = {
        'consumer_key': '30yDIVLPNSqep66EJFQqIEu3q',
        'consumer_secret': 'gBsN3NDZG36vQMiO7gETjSlzI4t793y9xbO4Mslk3n5Q6TSk3T',
        'access_token_key': '1631340727-LNg4SN1xteHhSrlK80aTQZKTzOvsyZtTnmQM2M5',
        'access_token_secret': 't7M2QNci0OCETNrubu9hfB8EAQidz1K4t6qSB8WuZF3w5'
    }
    
    def __init__(self,  request_limit = 20):
        self.request_limit = request_limit
        self._set_api()
        
    def _set_api(self):
        self.api = twitter.Api(
            consumer_key =   self.twitter_keys['consumer_key'],
            consumer_secret =   self.twitter_keys['consumer_secret'],
            access_token_key     =   self.twitter_keys['access_token_key'],
            access_token_secret  =   self.twitter_keys['access_token_secret']
        )
        
    def _mapper(self, item):
        return {
            'tweet_id': item.id,
            'handle': item.user.name,
            'retweet_count': item.retweet_count,
            'text': item.text,
            'mined_at': datetime.datetime.now(),
            'created_at': item.created_at
        }
    
    def mine_tweets_by_keyword(self, term='', lang='pt', since='', result_type='mixed'):
        statuses = self.api.GetSearch(term=term, count=100, lang=lang, since=since, result_type=result_type)
        
        return list(map(self._mapper, statuses))

def save_as_csv(dataset):
    df = pd.DataFrame(dataset)
    now = str(int(datetime.datetime.now().timestamp())) + str(random.randint(1,100001))
    df.to_csv('./csv/' + now + '.csv')
    
def iterator():
    py_twitter = TwitterMiner()
    threading.Timer(10.0, iterator).start()
    data = py_twitter.mine_tweets_by_keyword(term='bolsonaro', result_type='recent', since='2018-10-25')
    save_as_csv(data)
    data2 = py_twitter.mine_tweets_by_keyword(term='haddad', result_type='recent', since='2018-10-25')
    print('Completed an iteration at {0}!'.format(datetime.datetime.now().timestamp()))

iterator()