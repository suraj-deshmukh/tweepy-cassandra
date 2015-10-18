import tweepy
from cassandra.cluster import Cluster
#from tweepy import OAuthHandler

class client(object):
 def __init__(self,nodes=None):
  self.keyspace_name = None
  self.table_name = None
  self.string = None
  cluster = Cluster(nodes)
  self.session = cluster.connect()
 def create_keyspace(self,keyspace_name,table_name):
  self.keyspace_name = keyspace_name
  self.table_name = table_name 
  self.session.execute(("create keyspace %s with replication ={ 'class':'SimpleStrategy','replication_factor':3 };")%(self.keyspace_name))
  self.session.execute(("use %s")%(self.keyspace_name))
  self.session.execute(("create table %s( id int PRIMARY KEY ,tweets text);")%(self.table_name))
 def create_data(self,ids,tweet):
  self.string = "insert into %s (id,tweets) values"%(self.table_name)
  self.string = self.string + "(%s,%s)"
  for i in range(0,len(ids)):
   self.session.execute(self.string,parameters=[ids[i],tweet[i]])
 def load_data(self):
  result=self.session.execute("select * from %s"%(self.table_name))
  return(result)

consumer_key = '4m891a6vfSXLPS4zqPbMaI8nM'
consumer_secret = 'JxeoWzECAliGyzIWxabLAinsmxoB48fwFapQ7cqixyXeAQCelV'
access_token = '400557476-cgR5U27beRVjcLpETfM68bi5cCSTS8e2nKKzIwR0'
access_secret = '0eORxlTZh03xttApUAAzzx0UTqEBJiFajwQ00qwv3SLRb'
 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)
tweets=[]

for status in tweepy.Cursor(api.home_timeline).items(10):
    # Process a single status
    print(status.text) 
    tweets.append(status.text)


ids = range(0,len(tweets))
obj = client()
obj.create_keyspace("twitter","DATA")
result = obj.create_data(ids,tweets)
print(result)




