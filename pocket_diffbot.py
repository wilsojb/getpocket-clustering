'''
'''
import os
import sys

import bs4
import json
import pprint
import requests
import time
import webbrowser

import pymongo
import logging

# diffbot python client
sys.path.append('diffbot/')
import client

# headers needed to fool certains sites into thinking I'm a browser
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) '
  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36'}

# global client. only initialized once.
_MONGO_CLIENT = None

def getMongoClient():
  global _MONGO_CLIENT
  if _MONGO_CLIENT is None:
    try:
      mongo_uri = 'mongodb://..../pocket_data'
      mongo_connection = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
      _MONGO_CLIENT = mongo_connection['pocket_data']
    except Exception as e:
      logging.exception(e)
  return _MONGO_CLIENT

def sendToMongo(articles):
  try:
    mongo_db = getMongoClient()
    _ids = mongo_db['from_pocket'].insert(articles)
    print "New mongo _ids: %s" % _ids
  except pymongo.errors.OperationFailure:
    # happens when the user does not have permission to insert
    pass
  except pymongo.errors.ServerSelectionTimeoutError:
    # happens when the connection is invalid, or times out.
    pass
  except Exception as e:
    logging.exception(e)


def stripLinks(html):
  if html is None:
    return []

  soup = bs4.BeautifulSoup(html, 'html.parser')
  links = set()
  for link in soup(['a']):
    try:
      href = link['href']
      if href.startswith('http'):
        links.add(href)
    except:
      pass
  return list(links)

def getDiffbotResponse(url):
  if url is None:
    return {}

  diffbot = client.DiffbotClient()
  token = "..."
  version = 2
  api = "analyze"
  return diffbot.request(url, token, api, version=version)


def getPocketArticles(skip_if_available=False):

  if skip_if_available and os.path.isfile('data/pocket_data_raw.json'):
    with open('data/pocket_data_raw.json', 'rb') as fp:
      contents = json.load(fp)
      # print a sample before returning
      # pprint.pprint(contents['list'].itervalues().next())
      # print
      return contents

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
  with open('data/pocket_data_raw.json', 'wb') as fp:
    json.dump(data, fp)

  # print a sample before returning
  # pprint.pprint(data['list'].itervalues().next())
  # print
  return data


if __name__ == '__main__':

  # retreive all pocket data using API
  pocket_data = getPocketArticles(skip_if_available=True)

  articles_with_annotations = []

  for idx, article in enumerate(pocket_data['list'].itervalues()):
    # if idx > 1:
      # break

    # parse using diffbot API
    response = getDiffbotResponse(article.get('resolved_url', None))
    article['diffbot_response'] = response
    pprint.pprint(response)

    # extract links from response html body
    links = stripLinks(response.get('html', None))
    article['extracted_links'] = links

    # clean up time.com tags. ugh
    if 'tags' in article and 'time.com' in article['tags']:
      tag = article['tags']['time.com']
      del article['tags']['time.com']
      article['tags']['time_com'] = tag

    # buffer articles before sending to mongo in bulk
    articles_with_annotations.append(article)
    if len(articles_with_annotations) > 10:
      sendToMongo(articles_with_annotations)
      articles_with_annotations = []


