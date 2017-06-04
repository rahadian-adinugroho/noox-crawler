import os
import json
import hashlib
import pymysql
from modules import LinkExtractor, NewsGrabber
from output_providers import NooxSqlProvider, JsonProvider


def check_with_db(urls):
    db = pymysql.connect('localhost', 'root', '', 'nooxdbapi')
    cursor = db.cursor()
    sql = 'SELECT `url` FROM `news` WHERE `url_hash` IN ({0})'
    in_p = ', '.join(map(lambda x: "'" + hashlib.md5(x.encode('utf-8')).hexdigest() + "'", urls))
    sql = sql.format(in_p)
    print(sql)
    # print(sql)
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]

conf_dir = './config/liputan6.conf.json'
if os.path.isfile(conf_dir):
    with open(conf_dir) as conf_file:
        config = json.load(conf_file)
        # print(config)
        a = LinkExtractor(config)
        # print(a.get_urls(1))
        links = a.get_urls(max_link=50)
        grabber = NewsGrabber(config)
        news = grabber.process(links[:30], url_check_callback=check_with_db)
        print('Scanned '+str(len(news))+' out of '+str(len(links))+' links...')

        file = JsonProvider('liputan6.json', True, news)
        print('Writing to file...')
        file.save()

        out = NooxSqlProvider({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdbapi'}, config['noox_config'], news)
        print('Saving to database...')
        ids = out.save()
        print(ids)
