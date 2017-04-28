from bs4 import BeautifulSoup
import requests
import re

header = {
'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
'Accept-Encoding': 'none',
'Accept-Language': 'en-US,en;q=0.8',
'Connection': 'keep-alive'
}

url   = 'http://kompas.com'
regex = r"^http.*(?:\/([a-zA-Z]*)\.(?:.*kompas\.com\/read\/.*))$"

print('Retrieving data from url...')
page = requests.get(url, headers= header)

soup = BeautifulSoup(page.text, 'html5lib')

linkTags = soup.findAll('a', href=re.compile(regex))

links = set(map(lambda link: link['href'], linkTags))
print('Found '+str(len(links))+' links...')

file = open('detik_url2_dump.txt', 'w')

print('Writing to file...')
for link in links:
	subd = re.search(regex, link).group(1)
	file.write(subd+" : "+link+'\n')

file.close()

print('Operation finished...')