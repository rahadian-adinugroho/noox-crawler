import pymysql
import hashlib
from output_providers import BaseProvider


class NooxSqlProvider(BaseProvider):

    def __init__(self, config, data=None):
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
        if data is None:
            data = self.data
        if len(data) < 1:
            raise ValueError('No data to output!')

        cursor = self._db.cursor()
        baseSql = 'INSERT INTO `news` (`title`, `source_id`, `cat_id`, `url`, `url_hash`, `author`, `pubtime`, `content`) VALUES {0};'
        in_p = ', '.join(map(lambda x: "('"+self._db.escape_string(x['title'])+"', 3, 1, '"+self._db.escape_string(x['url'])+"', '"+self._db.escape_string(self._get_md5(x['url']))+"', '"+self._db.escape_string(x['author'])+"', '"+self._db.escape_string(x['pubtime'])+"', '"+self._db.escape_string(x['content'])+"')", data))
        sql = baseSql.format(in_p)
        # print(sql)
        cursor.execute(sql)
        self._db.commit()
        self.lastinsertids = [i for i in range(cursor.lastrowid, cursor.lastrowid + len(data))]
        return self.lastinsertids

    def _get_md5(self, string: str):
        m = hashlib.md5()
        m.update(string.encode('utf8'))
        return m.hexdigest()
