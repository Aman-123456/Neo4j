import json
import glob
from pprint import pprint
from py2neo import Graph
#connection is done here to neo4j
#'neo4j' is username and 'aman1234' is the password
graph = Graph(password="aman1234")
#connection is done here
#graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
list_tweet = []
data = json.load(open("/home/aman/test/dataset.json"))

x = """
CREATE CONSTRAINT ON (s:USER) ASSERT ( s.auth_screen_name) IS UNIQUE
"""
y = """
CREATE CONSTRAINT ON (s:TWEET) ASSERT ( s.tweetid) IS UNIQUE
"""
z = """
CREATE CONSTRAINT ON (s:LOCATION) ASSERT ( s.Location) IS UNIQUE
"""

graph.run(x)
graph.run(y)
graph.run(z)
query0="""
UNWIND {json} as tweet
	merge(u:User {auth_screen_name:tweet.author_screen_name})
	merge(t:Tweet {tweetid:tweet.tid})
        
        create(u)-[r:TWEETS]->(t)
        

"""    

query2="""
UNWIND {json} as tweet
        merge(t:Tweet {tweetid:tweet.tid})
        merge(u:User {auth_screen_name:tweet.mentions})
        create(t)-[r:MENTIONS]->(u)
       
"""

for i in data.values():
	list_tweet.append(i)

l=0
print l

graph.run(query0,json = list_tweet)

for i in list_tweet:	
        dict1={}
        l+=1
        print l
        if i['mentions'] is not None:
		for j in i['mentions'] :
			dict1['tid']=i['tid']
                        dict1['mentions']=j
                        graph.run(query2,json =dict1)
	








