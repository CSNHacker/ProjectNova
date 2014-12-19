import urllib2
import requests
from bs4 import BeautifulSoup
import re
import os.path

page = urllib2.urlopen("http://www.cnn.com")
soup = BeautifulSoup(page)

page2 = urllib2.urlopen("http://bbcworld.com")
soup2 = BeautifulSoup(page2)

#Finding the RSS feeds
link1 = soup.find('link',type='application/rss+xml')
link2 = soup2.find('link',type='application/rss+xml')


# soup = BeautifulSoup(open(link1['href']));
soup= BeautifulSoup(urllib2.urlopen(link1['href'])); #CNN link
soup2 = BeautifulSoup(urllib2.urlopen("http://feeds.bbci.co.uk/news/rss.xml?edition=us"));


#For BBC Clustering of RSS
dict1 = {};
title = soup2.find_all('title');
guid_link = soup2.find_all('guid');
description = soup2.find_all('description')


num=0
for i in title:
	if i.string not in dict1.keys():	
			#print description[num]	
			dict1.update({i.string:description[num].string})
			num=num+1

i=0
k=1
save_path = '/Users/hadoop/Documents/Project_Nova/BBC'
comName = os.path.join(save_path, "BBC_Index_FileName_Matching.txt")
for key,value in dict1.items():
	try:
		completeName = os.path.join(save_path, str(k)+"_"+str(i)+".txt")
		text_file = open(completeName,"w")
		text_file1 = open(comName,"a")
		# print (key, value)
		text_file.write(value)
		text_file1.write(str(k)+"_"+str(i)+"\t"+key+"\n")
		i=i+1;
		print "\n"
	except Exception:
		continue

#For NYT clustering of RSS
dict2={};
title = soup.find_all('title');
guid_link = soup.find_all('guid');
description = soup.find_all('description')

num=0

for i in title:
	if i.string not in dict2.keys():
		try:	
			# print str(description[num].string)
			k=re.split('[<]', str(description[num+1].string))
			# print k[0]
			dict2.update({i.string:k[0]})
			num=num+1
		except Exception:
			continue


i=0
k=2;
save_path = '/Users/hadoop/Documents/Project_Nova/CNN'
comName = os.path.join(save_path, "CNN_Index_FileName_Matching.txt")     
for key,value in dict2.items():
	completeName = os.path.join(save_path, str(k)+"_"+str(i)+".txt")
	text_file = open(completeName,"w")
	text_file1 = open(comName,"a")
	# print (key, value)
	text_file.write(value)
	text_file1.write(str(k)+"_"+str(i)+"\t"+key+"\n")
	i=i+1;
	print "\n"
