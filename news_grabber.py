import os
import json
import re
import requests
import pymysql
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urlparse
from dateutil.parser import parser as dp
from collections import deque


class NewsGrabber:

    _header = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q =0.8',
                'Connection': 'keep-alive'
            }

    _spc_chars = {"\n": "", "\t": "", "\r": "", "\r\n": ""}

    # remove script, style, and another tag.
    _base_regex = r'(?:<script(?:\s|\S)*?<\/script>)|(?:<style(?:\s|\S)*?<\/style>)|(?:<!--(?:\s|\S)*?-->)'

    def __init__(self, config, export_to='DB'):
        self._config = config
        self.__export_to = export_to

        regex = self._base_regex
        # if regex to remove tag is not empty, add it to the base regex with | (or) separator.
        if "article_regex_remove" in self._config:
            regex += '|'+'|'.join(self._config["article_regex_remove"])

        # if regex to replace tag is not empty, add it to the exception list.
        if "article_tag_replace" in self._config:
            self._config["article_tag_replace"].update(self._spc_chars)
            regex += '|'+''.join(map(lambda tag: '(?!'+re.escape(tag)+')', self._config["article_tag_replace"]))+r'(?:<\/?(?:\s|\S)*?>)'
        else:
            regex += '|'+r'(<\/?(\s|\S)*?>)'
        self.__compiled_regex = re.compile(regex)

        self.__db = pymysql.connect('localhost', 'root', '', 'nooxdb')

    def process(self, url_list, export_to='DB'):
        # check if url_list is an instance of list
        if not isinstance(url_list, list):
            raise TypeError('url_list is expected to be an instance of list')
        # use deque for better performance
        urls = deque(url_list)
        ret = []
        while urls:
            # empty the buffer for each iteration
            buffer_ = []

            # we need to check whether the news article is already in database or not
            while urls and len(buffer_) < 25:
                # fill the buffer to 10 (pass by reference)
                self._fill_buffer(buffer_, urls)

                # retrieve urls already in database
                inDB = self._check_with_db(buffer_)

                # we select url that is not in database yet
                buffer_ = [url for url in buffer_ if url not in inDB]

            # after the buffer is full and the news is not in the database, retrieve the data
            for url in buffer_:
                print(url)
                try:
                    req_data = requests.get(url, self._header)
                except Exception as e:
                    continue

                tags = []
                tags.append(self._config["title_tag"])
                tags.append(self._config["date_tag"])
                tags.append(self._config["article_tag"])
                soup = BeautifulSoup(req_data.text, 'lxml', parse_only=SoupStrainer(tags))

                if "title_attr" in self._config and self._config["title_attr"] is not None:
                    title = soup.find(self._config["title_tag"], {self._config["title_attr"]: re.compile(self._config["title_attr_val"])}).get_text()
                else:
                    title = soup.find(self._config["title_tag"]).get_text()
                if title is None or len(title) < 5:
                        print('url "{0}" title is "{1}"'.format(url, title))
                        continue

                try:
                    if "date_attr" in self._config and self._config["date_attr"] is not None:
                        date = soup.find(self._config["date_tag"], {self._config["date_attr"]: re.compile(self._config["date_attr_val"])}).get_text()
                    else:
                        date = soup.find(self._config["date_tag"]).get_text()
                    sqlDate = self._date_parser(date)
                    if sqlDate is None:
                        print('url "{0}" date is "{1}"'.format(url, sqlDate))
                        continue
                except Exception as e:
                    print('unable to scan "{0}" because "{1}"'.format(url, str(e)))
                    continue

                if "article_attr" in self._config:
                    article_parts = soup.findAll(self._config["article_tag"], {self._config["article_attr"]: re.compile(self._config["article_attr_val"])})
                else:
                    article_parts = soup.findAll(self._config["article_tag"])

                article = ''
                for part in article_parts:
                    cleantext = re.sub(self.__compiled_regex, '', str(part))

                    if "article_tag_replace" in self._config:
                        article += self._multireplace(cleantext, self._config["article_tag_replace"])
                    else:
                        article += self._multireplace(cleantext, spc_chars)
                if article is None or len(article) < 5:
                    print('url "{0}" content is "{1}"'.format(url, article))
                    continue
                # print(article)
                ret.append({'title': title, 'url': url, 'pubtime': sqlDate, 'content': article})
                # print({'title': title, 'text': article})
        return ret

    def _fill_buffer(self, buffer_, urls):
        while urls and len(buffer_) < 25:
            url = urls.popleft()
            if self._config['sitename'] == self._get_domain_name(url):
                buffer_ += [url]

    def _check_with_db(self, buffer_):
        cursor = self.__db.cursor()
        sql = 'SELECT `url` FROM `news` WHERE `url` IN ({0})'
        in_p = ', '.join(map(lambda x: "'"+x+"'", buffer_))
        sql = sql.format(in_p)
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]

    def _date_parser(self, date):
        """
        Attempt to parse a date from a given string.
        """
        if not isinstance(date, str):
            raise TypeError('date argument is expected to be a string')
        month = {
            'january': '01',
            'february': '02',
            'march': '03',
            'april': '04',
            'may': '05',
            'june': '06',
            'july': '07',
            'august': '08',
            'september': '09',
            'october': '10',
            'november': '11',
            'december': '12'
        }
        shortMonth = {
            'jan': '01',
            'feb': '02',
            'mar': '03',
            'apr': '04',
            'jun': '06',
            'jul': '07',
            'aug': '08',
            'ags': '08',
            'sep': '09',
            'oct': '10',
            'nov': '11',
            'dec': '12'
        }
        bulan = {
            'januari': '01',
            'februari': '02',
            'maret': '03',
            'april': '04',
            'mei': '05',
            'juni': '06',
            'juli': '07',
            'agustus': '08',
            'september': '09',
            'oktober': '10',
            'nopember': '11',
            'desember': '12'
        }
        if self._config["normalize_date"] is True:
            repl = {}
            repl.update(month)
            repl.update(shortMonth)
            repl.update(bulan)
            date = self._multireplace(date.lower(), repl)
        if "date_regex" in self._config and len(self._config["date_regex"]) > 0:
            reg = re.compile(self._config["date_regex"])
            try:
                matches = reg.search(date).groupdict()
                date = '{y}/{m}/{d} {h}:{i}'.format_map(matches)
            except Exception as e:
                date = re.sub(r'[^0-9:\s\/\-]', '', date)
        else:
            date = re.sub(r'[^0-9:\s\/\-]', '', date)

        try:
            parser = dp()
            dateObj = parser.parse(date)
            return dateObj.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return None

    def _multireplace(self, string, replacements):
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

    def _get_domain_name(self, url):
        """
        Given a url, return domain name (www.example.com returns example).
        :param url string: input url
        :rtype: str
        """
        url_parts = urlparse(url).hostname.split('.')
        return url_parts[1 if len(url_parts) == 3 else 0]

# urls = [
#         'https://inet.detik.com/consumer/d-3496637/snapdragon-660-dan-630-jadi-jagoan-baru-qualcomm',
#         'http://nasional.kompas.com/read/2017/04/27/11333611/ada.setya.novanto.di.balik.proyek.e-ktp.pengusaha.ini.tolak.ikut.lelang',
#         'https://news.detik.com/berita/3487026/ini-nama-nama-anggota-dpr-inisiator-angket-kpk'
#     ]

# conf_dir = './config/kompas.conf.json'
# if os.path.isfile(conf_dir):
#     with open(conf_dir) as conf_file:
#         config = json.load(conf_file)
#         regex = re.compile(config['url_regex'])
#         # print(config)
#         a = NewsGrabber(config)
#         # print(a.get_urls(1))
#         a.process(urls)
