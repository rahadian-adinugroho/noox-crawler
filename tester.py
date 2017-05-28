import os
import json
import re
from modules.link_extractor import LinkExtractor
from news_grabber import NewsGrabber
# from dateutil.parser import parser as dateparse

conf_dir = './config/detik.conf.json'
if os.path.isfile(conf_dir):
    with open(conf_dir) as conf_file:
        config = json.load(conf_file)
        regex = re.compile(config['url_regex'])
        # print(config)
        a = LinkExtractor(config)
        # print(a.get_urls(1))
        links = a.get_urls(max_link=50)
        grabber = NewsGrabber(config)
        news = grabber.process(links)

        file = open('detik_url2_dump.txt', 'w')
        print('Writing to file...')
        # for data in news:
        #     groups = regex.search(link)
        #     if groups is None:
        #         subd = 'ERR'
        #     else:
        #         subd = groups.group(1)
        #     file.write(subd+" : "+link+'\n')
        # file.write(str(links))
        json.dump(news, file)
        file.close()
