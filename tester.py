import os
import json
from modules import LinkExtractor, NewsGrabber
from output_providers import NooxSqlProvider, JsonProvider
# from dateutil.parser import parser as dateparse

conf_dir = './config/liputan6.conf.json'
if os.path.isfile(conf_dir):
    with open(conf_dir) as conf_file:
        config = json.load(conf_file)
        # print(config)
        a = LinkExtractor(config)
        # print(a.get_urls(1))
        links = a.get_urls(max_link=50)
        grabber = NewsGrabber(config)
        news = grabber.process(links[:30])
        print('Scanned '+str(len(news))+' out of '+str(len(links))+' links...')

        file = JsonProvider('liputan6.json', True, news)
        print('Writing to file...')
        file.save()

        out = NooxSqlProvider({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdbapi'}, config['noox_config'], news)
        print('Saving to database...')
        ids = out.save()
        print(ids)
