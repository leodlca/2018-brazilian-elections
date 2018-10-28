import re, threading, datetime, glob, json, pandas as pd

def get_filenames(path='./', extension='.csv'):
    return glob.glob(path + "/*.csv")

def merge_and_filter_all(filenames):
    start = datetime.datetime.now().timestamp()
    df_arr = []
    
    for f in filenames:
        df_arr.append(pd.read_csv(f,  index_col='tweet_id'))
    merged = pd.concat(df_arr)
    final = merged[~merged.index.duplicated(keep='first')]
    
    end = datetime.datetime.now().timestamp()
    final.to_csv('merged' + str(end) + '.csv')
    print('Merged and filtered! It only took ' + str(end-start) + 'sec.')
    return final

def map_df(df):
    return df.filter(items=['tweet_id', 'created_at', 'retweet_count', 'text'])

merge_and_filter_all(get_filenames(path='./csv/'))