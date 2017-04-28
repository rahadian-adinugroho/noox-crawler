import os
import json
import re
import requests

from bs4 import BeautifulSoup
from urllib.parse import urlparse

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

def get_domain_name(url):
	"""
    Given a url, return domain name (www.example.com returns example).
    :param url string: input url
    :rtype: str
    """
	url_parts = urlparse(url).hostname.split('.')
	return url_parts[1 if len(url_parts) == 3 else 0]

header = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
		'Accept-Encoding': 'none',
		'Accept-Language': 'en-US,en;q =0.8',
		'Connection': 'keep-alive'}

urls = [
	'http://nasional.kompas.com/read/2017/04/27/18550201/baru.bebas.3.tahun.fahd.el.fouz.kembali.jadi.tersangka.di.kpk',
	'http://nasional.kompas.com/read/2017/04/27/11333611/ada.setya.novanto.di.balik.proyek.e-ktp.pengusaha.ini.tolak.ikut.lelang',
	'https://news.detik.com/berita/3487026/ini-nama-nama-anggota-dpr-inisiator-angket-kpk'
]

base_regex = r'(?:<script(?:\s|\S)*?<\/script>)|(?:<style(?:\s|\S)*?<\/style>)|(?:<!--(?:\s|\S)*?-->)'
cur_config = {}
compiled_regex = None

for url in urls:
	cur_url_domain = get_domain_name(url)
	if len(cur_config) == 0 or cur_config["sitename"] != cur_url_domain:
		print('Loading config for: '+cur_url_domain)
		conf_dir = './config/'+cur_url_domain+'.conf.json'
		if os.path.isfile(conf_dir):
			with open(conf_dir) as conf_file:
				cur_config = json.load(conf_file)
				print('building regex...')
				regex = base_regex
				if "article_regex_remove" in cur_config:
					regex+='|'+'|'.join(cur_config["article_regex_remove"])

				if "article_tag_replace" in cur_config:
					regex+='|'+''.join(map(lambda tag: '(?!'+re.escape(tag)+')', cur_config["article_tag_replace"]))+r'(?:<\/?(?:\s|\S)*?>)'
				else:
					regex+='|'+r'(<\/?(\s|\S)*?>)'

				compiled_regex = re.compile(regex)
		else:
			raise Exception(cur_url_domain+".conf.json not found in config directory")

	# r = re.search(confList["detik"]["url_regex"], url).group(1) # for categorizing

	print('retrieving page data...')
	req_data = requests.get(url, header)
	soup = BeautifulSoup(req_data.text, 'lxml')
	
	if "article_attr" in cur_config:
		article_parts = soup.findAll(cur_config["article_tag"], {cur_config["article_attr"]: re.compile(cur_config["article_attr_val"])})
	else:
		article_parts = soup.findAll(cur_config["article_tag"])

	print("cleaning article...")
	article = ''
	for part in article_parts:
		cleantext = re.sub(compiled_regex, '', str(part))
		if "article_tag_replace" in cur_config:
			article+=multireplace(multireplace(cleantext, cur_config["article_tag_replace"]), {"\n" : "", "\t" : ""})
	print(article)
# print(article)