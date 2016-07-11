#!/anaconda/bin/python
'''
Run:    python pocket_onboarding.py

Input:  Https://getpocket.com account credentials

Output: Three DataFrames stored as pocket_data_raw.pkl, pocket_data.pkl, and pocket_links.pkl. 
        One JSON file stored as pocket_data_raw.json
        
        1) pocket_data_raw.pkl contains columns of meta data described on
           https://getpocket.com/developer/docs/v3/retrieve as well as 'article_text' 
           which is the body of text extracted using beautifulsoup/requests on 'resolved_url'

        2) pocket_data.pkl is a cleaned up version of pocket_data_raw.pkl. See below (def cleanData)
           for the full list of cleaning steps that were taken, but for example this includes
           eliminating null values from important fields and making assumptions about 'article_text'
           word counts. 

        3) pocket_links.pkl is a two column data frame that maps (many-to-many) 'resolved_url' to
           links found while scraping 'resolved_url'. 
'''

import bs4
import collections
import json
import numpy as np
import pandas as pd
import pprint
import re
import requests
import time
import webbrowser

# headers needed to fool certains sites into thinking I'm a browser
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) '
  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36'}


def getPocketArticles():
  consumer_key = "..."
  redirect_uri = "https://getpocket.com/developer/docs/v3/retrieve"
  request_headers = {'Content-Type' : 'application/json','X-Accept': 'application/json'}
  refresh_token = None
  access_token = None
  username = None

  # Obtain a refresh token
  request_data = json.dumps({"consumer_key":consumer_key, "redirect_uri":redirect_uri})
  response = requests.post('https://getpocket.com/v3/oauth/request', 
    data=request_data, headers=request_headers)

  if response.status_code != 200:
    print "%d: %s" % (response.status_code, response.text)
    print
  else:
    refresh_token = response.json().get('code', None)
    print "refresh_token=%s" % refresh_token
    print

  # Have the user authorize the refresh_token
  webbrowser.open('https://getpocket.com/auth/authorize?request_token=%s&redirect_uri=%s' 
    % (refresh_token, redirect_uri))
  time.sleep(5)

  # Obtain an access token
  request_data = json.dumps({"consumer_key":consumer_key,"code":refresh_token})
  response = requests.post('https://getpocket.com/v3/oauth/authorize', 
    data=request_data, headers=request_headers)

  if response.status_code != 200:
    print "%d: %s" % (response.status_code, response.headers)
    print
  else:
    access_token = response.json().get('access_token', None)
    username = response.json().get('username', None)
    print "access_token=%s" % access_token
    print "username=%s" % username
    print

  # Retrieve list of pocketed articles
  request_data = json.dumps({"consumer_key":consumer_key, 
    "access_token":access_token,"state":"all", "detailType":"complete"})
  response = requests.post('https://getpocket.com/v3/get', 
    data=request_data, headers=request_headers)

  data = response.json()

  # store JSON to disk
  with open('pocket_data_raw.json', 'wb') as fp:
    json.dump(data, fp)

  # print a sample before returning
  pprint.pprint(data['list'].itervalues().next())
  print
  return data


def extractTextAndLinks(text):
  # strip the soup of expected, uninteresting tags
  soup = bs4.BeautifulSoup(text, 'lxml')
  [s.extract() for s in soup(['script', 'meta', 'link', 'title'])]

  def getText(s):
    body = " ".join(re.sub(r'[^a-z]', '', word.lower()) 
      for word in s.get_text().split())
    return body

  def getLinks(s):
    links = set()
    for link in s(['a']):
      try:
        href = link['href']
        if href.startswith('http'):
          links.add(href)
      except:
        pass
    return links

  return getText(soup), getLinks(soup)


def makeDataFrames(data):
  # extract text and new links from pocket data
  new_links = []
  for _id, article in data['list'].iteritems():
    if 'resolved_url' not in article:
      continue
    try:
      response = requests.get(article['resolved_url'], headers=headers)
    except Exception as e:
      continue  
    if response.status_code != 200:
      continue

    text, links = extractTextAndLinks(response.text)
    data['list'][_id]['article_text'] = text

    for link in links:
      new_links.append({
        'resolved_id': data['list'][_id]['resolved_id'],
        'found_link': link
      })

  # translate to pandas and store raw versions
  df = pd.DataFrame(pocket_data['list'].values())
  new_links = pd.DataFrame(new_links)
  print "Shape of Pocket DataFrame: ", df.shape
  print "Shape of Links DataFrame: ", new_links.shape
  print

  df.to_pickle('pocket_data_raw.pkl')
  new_links.to_pickle('pocket_links.pkl')
  return df, new_links

def cleanData(df):
  # clean all missing article_text - not sure why this happens
  df = df[pd.notnull(df['article_text'])]
  
  # translate binary fields to true/false
  df['is_archived'] = df.status.map(lambda x: int(x) !=0)
  df['actual_word_count'] = df.article_text.map(lambda x: len(x.split()))

  # select only the columns we are interested in using
  df = df[['resolved_id', 'word_count', 'actual_word_count', 'resolved_title', 
    'resolved_url', 'article_text', 'is_archived', 'excerpt', 'tags']]

  # TfidfVectorizer, which will most likely be used to featurize, 
  # doesn't work well on smaller samples. Also, pages that depend
  # mostly on javascript can have zero words in either column.
  df = df[df.word_count.astype(int) > 100]
  df = df[df.actual_word_count.astype(int) > 100]    

  # find the percent difference between the two counts
  percent_diffs = np.abs( df.actual_word_count.astype(int) - df.word_count.astype(int) )
  percent_diffs = ( percent_diffs * 100 ) / df.word_count.astype(int)
  df['percent_diffs'] = percent_diffs

  # exclude anything greater than 300% difference in word count
  df = df[df.percent_diffs < 300]

  # view the results of these transformations
  print "Shape of Cleaned DataFrame: ", df.shape
  print

  df.to_pickle('pocket_data.pkl')
  return df


if __name__ == '__main__':

  # retreive all pocket data using API
  pocket_data = getPocketArticles()

  # create two dataframes from pocket data. one 
  # for the pocket data + article text and the other
  # being a map of pocket resolved id --> found url
  data, links = makeDataFrames(pocket_data)

  # cleans and translates the raw into a format more suitable
  # for processing
  data = cleanData(data)



