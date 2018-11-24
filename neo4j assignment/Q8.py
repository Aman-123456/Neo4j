import json
import glob
from pprint import pprint
from py2neo import Graph
graph = Graph(password="aman1234")
#graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
tweetslist = []
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
	merge(aut:Author {auth_screen_name:tweet.author_screen_name})
	merge(t:Tweet {tweetid:tweet.tid})
        
        create(aut)-[r:TWEETS]->(t)
        

"""    

query2="""
UNWIND {json} as tweet
        merge(t:Tweet {tweetid:tweet.tid})
        merge(h:Hashtag {hashtag:tweet.hashtags})
        create(h)-[r:intweetid]->(t)
       
"""

for i in data.values():
	tweetslist.append(i)

l=0
print l

graph.run(query0,json = tweetslist)

for i in tweetslist:	
        dict1={}
        l+=1
        print l
        if i['hashtags'] is not None:
		for j in i['hashtags'] :
			dict1['tid']=i['tid']
                        dict1['hashtags']=j
                        graph.run(query2,json =dict1)
	



result=graph.run('''MATCH (u1:User)-[:TWEETS]->(t1:Tweet)<-[:intweetid]-(h1:Hashtag)
       WHERE h1.hashtag='Sridevi'
       return h1.hashtag,u1.auth_screen_name,COLLECT(t1.tweetid),Count(*) As count
       ORDER BY count DESC;''')

print result




