import glob
import os
import json
import re
import requests
from bs4 import BeautifulSoup


def multireplace(string, replacements):
    """
    Given a string and a replacement map, it returns the replaced string.
    :param str string: string to execute replacements on
    :param dict replacements: replacement dictionary {value to find: value to replace}
    :rtype: str
    """
    # Place longer ones first to keep shorter substrings from matching where the longer ones should take place
    # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against the string 'hey abc', it should produce
    # 'hey ABC' and not 'hey ABc'
    substrs = sorted(replacements, key=len, reverse=True)

    # Create a big OR regex that matches any of the substrings to replace
    regexp = re.compile('|'.join(map(re.escape, substrs)))

    # For each match, look up the new string in the replacements
    return regexp.sub(lambda match: replacements[match.group(0)], string)

header = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
		'Accept-Encoding': 'none',
		'Accept-Language': 'en-US,en;q=0.8',
		'Connection': 'keep-alive'}

# url = 'https://news.detik.com/berita/3487026/ini-nama-nama-anggota-dpr-inisiator-angket-kpk'
url = 'http://nasional.kompas.com/read/2017/04/27/18550201/baru.bebas.3.tahun.fahd.el.fouz.kembali.jadi.tersangka.di.kpk'
base_regex = r'(?:<script(?:\s|\S)*?<\/script>)|(?:<style(?:\s|\S)*?<\/style>)|(?:<!--(?:\s|\S)*?-->)'

confList = {}
for filename in glob.glob('./config/*.conf.json'):
	file = open(filename, 'r')
	siteName = os.path.basename(filename).split('.')[0]
	with open(filename) as conf_file:
		print(conf_file.read())
		conf_file.seek(0)
		confList[siteName] = json.loads(conf_file.read())
	print(os.path.basename(filename).split('.')[0])

# r = re.search(confList["detik"]["url_regex"], url).group(1)
result = True if "tag_replace" in confList["kompas"] else False

req_data = requests.get(url, header)
soup = BeautifulSoup(req_data.text, 'lxml')
config = confList["kompas"]
if "article_attr" in config:
	article_parts = soup.findAll(config["article_tag"], {config["article_attr"]: re.compile(config["article_attr_val"])})
else:
	article_parts = soup.findAll(config["article_tag"])

regex = base_regex
if "article_regex_remove" in config:
	regex+='|'+'|'.join(config["article_regex_remove"])

if "article_tag_replace" in config:
	regex+='|'+''.join(map(lambda tag: '(?!'+re.escape(tag)+')', config["article_tag_replace"]))+r'(?:<\/?(?:\s|\S)*?>)'
else:
	regex+='|'+r'(<\/?(\s|\S)*?>)'

print(regex)
article = ''
cleanr = re.compile(regex)
for part in article_parts:
	# print(str(part))
	cleantext = re.sub(cleanr, '', str(part))
	# print(cleantext)
	if "article_tag_replace" in config:
		# print(cleantext.replace('<p>', '<br><br>'))
		print(multireplace(cleantext, config["article_tag_replace"]))
	
# print(article)