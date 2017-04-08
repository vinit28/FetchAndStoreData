
# coding: utf-8

# In[ ]:

#!/usr/bin/python3


import requests


def getData():
    url = 'https://api.nytimes.com/svc/mostpopular/v2/mostshared/all-sections/1.json?api-key=62c1e7a54140488a9d812383a49cf928'
    results = requests.get(url).json()
    data = results['results']
    return data



# #Deliverable
# An iPython notebook with the code, and with some text explaining the data source and your objective (use the "File" => "Download As" => "Notebook (.ipynb)". Ensure that you upload the ipynb file.
# The name of your database together with the IP address of your machine, so that we can connect and inspect your database
# A few screenshots of your database, showing the data that you store and how they change over time (this is as a fallback, just in case we cannot connect to your database).
# 
# API used: New York Times Most Pupular (By Shares) & Watson's Alchemy API to extract URL Text Sentiment and URL Entity
# Database Name: NYTimes
# Table Name: Articles, Articles_Constant
# IP Address: 54.165.54.4
# 
# The python code collects the most shared articles on New York Times using the NYT Most Popular API. The next step is to analyze the text using the Watson API. One of the thins I do is to extract the most relevant entity and then get the overall sentiment of the article. This is done to determine what type of sentiment is positive and what's negative. What's the opinion of the media about certain topics and what is shared commonly, positive sentiment articles or negative. What I observed is that more negative sentiment articles are shared in terms of number of articles shared, however positive sentiment articles though fewer in quantity are shared more often. 
# 
# 
# 

# In[ ]:

#SETTING UP THE DATABASE CONNECTION
import MySQLdb as mdb
import sys

con = mdb.connect(host = 'localhost', 
                  user = 'root', 
                  passwd = 'dwdstudent2015', 
                  charset='utf8', use_unicode=True);

# Check for existing database
##db_name = 'NYTimes'
#drop_db_query = "DROP DATABASE IF EXISTS {db}".format(db=db_name)
#cursor = con.cursor()
#cursor.execute(drop_db_query)
#cursor.close()

# Create the database
def createDB():
    db_name = 'NYTimes'
    create_db_query = "CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET 'utf8'".format(db=db_name)

    cursor = con.cursor()
    cursor.execute(create_db_query)
    cursor.close()
    return


# In[ ]:

#Create time invariant table

def createInvariant():
    cursor = con.cursor()
    db_name = 'NYTimes'
    table1_name = 'Articles_Constant'
    # The {db} and {table} are placeholders for the parameters in the format(....) statement
    create_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table} 
                                    (url varchar(250), 
                                    title varchar(250), 
                                    published_date datetime,
                                    section varchar(250),
                                    entity varchar(250),
                                    sentiment_type varchar(250),
                                    Shares int,
                                    PRIMARY KEY(title)
                                    )'''.format(db=db_name, table=table1_name)
    cursor.execute(create_table_query)
    cursor.close()
    return

#CREATE timevaryingtable
def createVariant():
    cursor = con.cursor()
    db_name = 'NYTimes'
    table2_name = 'Articles'
    # The {db} and {table} are placeholders for the parameters in the format(....) statement
    create2_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table} 
                                    (url varchar(250), 
                                    title varchar(250), 
                                    published_date datetime,
                                    section varchar(250),
                                    entity varchar(250),
                                    sentiment_type varchar(250),
                                    Shares int,
                                    Time int,
                                    PRIMARY KEY(title, Time),
                                    FOREIGN KEY(title) REFERENCES {db}.Articles_Constant(title)

                                    )'''.format(db=db_name, table=table2_name)
    cursor.execute(create2_table_query)
    cursor.close()
    return


# In[ ]:


def getEntity(url):
    watson_url = "http://gateway-a.watsonplatform.net/calls/url/URLGetRankedNamedEntities"
    api_key = '4b46c7859a7be311b6f9389b12504e302cac0a55'
    headers = {
      "Accept": "application/json"
    }

    parameters = {
        'outputMode': 'json',
        'apikey' : api_key,
        'sentiment' :1,
        'knowledgeGraph': 1,
        'url': url
    }
    
    watson_resp = requests.post(watson_url, params=parameters, headers=headers)
    watson_result = watson_resp.json()
    watson_entity = str(watson_result["entities"][0]['text'])
    return watson_entity

def getSentiment(url):
    watson_url = "http://gateway-a.watsonplatform.net/calls/url/URLGetTextSentiment"
    api_key = '4b46c7859a7be311b6f9389b12504e302cac0a55'
    headers = {
      "Accept": "application/json"
    }

    parameters = {
        'outputMode': 'json',
        'apikey' : api_key,
        'url': url
    }
    
    watson_resp = requests.post(watson_url, params=parameters, headers=headers)
    watson_result = watson_resp.json()
    watson_sentiment = str(watson_result["docSentiment"]["type"])
    return watson_sentiment


# In[ ]:

#import data
import datetime
import time

def insertVariant():
    query_template = '''INSERT IGNORE INTO NYTimes.Articles(url, title, published_date, section, entity, sentiment_type, Shares, Time) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

    cursor = con.cursor()
    data = getData()

    for entry in data:
        url = entry["url"]
        title = entry["title"]
        date = entry["published_date"]
        section = entry["section"]
        Shares = entry['total_shares']
        entity = getEntity(url)
        sentiment_type = getSentiment(url)
        Time = int(time.time())
        query_parameters = (url, title, date, section, entity, sentiment_type, Shares, Time)
        cursor.execute(query_template, query_parameters)

    con.commit()
    cursor.close()
    return

###################
def insertInvariant():
    query_template = '''INSERT IGNORE INTO NYTimes.Articles_Constant(url, title, published_date, section, entity, sentiment_type, Shares) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    cursor = con.cursor()
    data = getData()
    
    
    for entry in data:
        url = entry["url"]
        title = entry["title"]
        date = entry["published_date"]
        section = entry["section"]
        Shares = entry['total_shares']
        entity = getEntity(url)
        sentiment_type = getSentiment(url)
        query_parameters = (url, title, date, section, entity, sentiment_type, Shares)
        cursor.execute(query_template, query_parameters)

    con.commit()
    cursor.close()
    return


# In[ ]:

createDB()

createInvariant()
insertInvariant()

createVariant()
insertVariant()



# In[ ]:




# In[ ]:



