import pymysql
import hashlib
import re
import requests
import os
from output_providers import BaseProvider


class NooxSqlProvider(BaseProvider):

    def __init__(self, config, noox_config=None, data=None):
        if not isinstance(config, dict):
            raise TypeError('config parameter is expected to be dict instance')
        if not all(key in ('db_url', 'db_username', 'db_password', 'db_name') for key in config):
            print(config)
            raise KeyError('Missing parameter in config')

        self.config = config
        if 'db_charset' in config:
            self.config.update({'db_charset': config['db_charset']})
        else:
            self.config.update({'db_charset': 'latin-1'})

        if noox_config is not None:
            self.set_noox_config(noox_config)
        else:
            self.noox_config = None

        self.data = data

        self._db = pymysql.connect(
            self.config['db_url'],
            self.config['db_username'],
            self.config['db_password'],
            self.config['db_name'],
            charset='utf8')

    def size(self):
        if self.data is not None:
            return len(self.data)
        else:
            return 0

    def put(self, data):
        self.data = data
        return self

    def save(self, data=None):
        if self.noox_config is None:
            raise RuntimeError('Noox config (noox_config) is not initialized.')

        if data is None:
            data = self.data
        if len(data) < 1:
            raise ValueError('No data to output!')

        filteredData = [item for item in data if self._get_category_id(item['url']) is not None]
        if len(filteredData) < 1:
            raise RuntimeError('Output data is zero after skipping data with no category.')

        cursor = self._db.cursor()
        baseSql = 'INSERT INTO `news` (`title`, `source_id`, `cat_id`, `url`, `url_hash`, `author`, `pubtime`, `content`) VALUES {0};'
        in_p = ', '.join([item for item in map(self._format_sql, filteredData)])
        sql = baseSql.format(in_p)
        # print(sql)
        cursor.execute(sql)
        self._db.commit()
        self.lastinsertids = [i for i in range(cursor.lastrowid, cursor.lastrowid + len(filteredData))]
        self._download_news_images([(id, filteredData[i]['img_url']) for i, id in enumerate(self.lastinsertids)])
        return self.lastinsertids

    def set_noox_config(self, noox_config):
        if not isinstance(noox_config, dict):
            raise TypeError('noox_config is expected to be dict type')

        self.noox_config = noox_config
        self._source_id = self._get_noox_news_source_id()
        self._noox_categories = self._get_noox_categories()
        self._url_regex = re.compile(self.noox_config['url_regex'])

    def _get_md5(self, string: str):
        m = hashlib.md5()
        m.update(string.encode('utf8'))
        return m.hexdigest()

    def _get_noox_categories(self):
        cursor = self._db.cursor()
        sql = 'SELECT * FROM `news_categories`'
        cursor.execute(sql)
        return dict((name, id) for id, name in cursor.fetchall())

    def _get_category_id(self, url):
        keyword = self._url_regex.search(url).group(1)
        if keyword in self.noox_config['categories']:
            category_name = self.noox_config['categories'][keyword]
        else:
            if self.noox_config['allow_default_category']:
                category_name = self.noox_config['default_category']
            elif self.noox_config['skip_when_no_category']:
                print('Skipping: {0} (no category)'.format(url))
                return None
            else:
                raise Exception('Cannot determine category from {0}'.format(url))
        return self._noox_categories[category_name]

    def _get_noox_news_source_id(self):
        cursor = self._db.cursor()
        sql = 'SELECT `id` FROM `news_sources` WHERE `source_name` = {0}'.format("'"+self._db.escape_string(self.noox_config['db_sitename'])+"'")
        if cursor.execute(sql) == 1:
            return cursor.fetchone()[0]
        else:
            raise Exception('Source id could not be found from database. Check your configuration file.')

    def _format_sql(self, item):
        category_id = self._get_category_id(item['url'])
        if category_id is not None:
            return "('"+self._db.escape_string(item['title'])+"', '"+str(self._source_id)+"', '"+str(category_id)+"', '"+self._db.escape_string(item['url'])+"', '"+self._get_md5(item['url'])+"', '"+self._db.escape_string(item['author'])+"', '"+self._db.escape_string(item['pubtime'])+"', '"+self._db.escape_string(item['content'])+"')"
        else:
            raise Exception('No categories reached _format_sql')

    def _download_news_images(self, items: list):
        for item in items:
            with open(self.noox_config['img_dir']+str(item[0])+'.jpg', 'wb') as file:
                try:
                    img = requests.get(item[1])
                    file.write(img.content)
                    file.close()
                except Exception as e:
                    print('Unable to download "{0}" cause: {1}'.format(item[1], str(e)))
                    os.unlink(file.name)
