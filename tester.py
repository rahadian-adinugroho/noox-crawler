import os
import json
import re
import pymysql
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
        news = grabber.process(links[:30])
        print('Scanned '+str(len(news))+' out of '+str(len(links))+' links...')
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
        json.dump(news, file, indent=4)
        file.close()

        db = pymysql.connect('localhost', 'root', '', 'nooxdb')
        cursor = db.cursor()
        sql = 'INSERT INTO `news` (`title`, `source_id`, `cat_id`, `url`, `author`, `pubtime`, `content`) VALUES {0};'
        in_p = ', '.join(map(lambda x: "('"+db.escape_string(x['title'])+"', 1, 1, '"+db.escape_string(x['url'])+"', 'anon', '"+db.escape_string(x['pubtime'])+"', '"+db.escape_string(x['content'])+"')", news))
        sql = sql.format(in_p)
        # print(sql)
        cursor.execute(sql)
        db.commit()