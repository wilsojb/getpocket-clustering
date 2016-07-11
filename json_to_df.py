'''
'''
import pandas as pd
import pymongo
import pprint

# make sure we display all the columns
pd.set_option('display.max_columns', 25)


def getCursor():
  mongo_connection = pymongo.MongoClient('mongodb://.../pocket_data', 
    serverSelectionTimeoutMS=3000)
  mongo_client = mongo_connection['pocket_data']
  return mongo_client['from_pocket'].find({})


def getDiffBotData(d):
  ret = {}
  try:
    response = d['diffbot_response']
  except KeyError:
    pass
  else:
    for key in ['text', 'resolved_url', 'title', 'type', 'excerpt']:
      if key in response:
        ret[key] = response[key]
  return ret


def getPocketData(d):
  ret = {}
  for key in ['resolved_id', 'resolved_title', 'resolved_url', 'excerpt', 'status']:
    try:
      ret[key] = d[key]
    except KeyError:
      pass
  return ret


if __name__ == '__main__':

  # main data structures of program
  links = []
  articles = []

  for datum in getCursor():
    diffbot = getDiffBotData(datum)
    pocket = getPocketData(datum)
    
    try:
      new_links = datum['extracted_links']
    except KeyError:
      pass
    else:
      for link in new_links:
        links.append({'resolved_id': datum['resolved_id'], 'link': link})
            
    # combine & append
    pocket['excerpt'] = pocket.get('excerpt', diffbot.get('excerpt', None))
    pocket['type'] = diffbot.get('type', None)
    pocket['resolved_title'] = pocket.get('resolved_title', diffbot.get('title', None))
    pocket['text'] = diffbot.get('text', None)
    pocket['resolved_url'] = pocket.get('resolved_url', diffbot.get('resolved_url', None))
    
    articles.append(pocket)

    
  # convert to DataFrame
  df = pd.DataFrame(articles)
  df_links = pd.DataFrame(links)

  # clean all missing article_text - not sure why this happens
  df = df[pd.notnull(df['text'])]
  
  # translate binary fields to true/false
  df['is_archived'] = df.status.map(lambda x: int(x) !=0)
  df['word_count'] = df.text.map(lambda x: len(x.split()))
  df = df.drop('status', 1)

  # TfidfVectorizer, which will most likely be used to featurize, 
  # doesn't work well on smaller samples.
  df = df[df.word_count.astype(int) > 100]
    

  df.to_pickle('data/df.pkl')
  df_links.to_pickle('data/df_links.pkl')

