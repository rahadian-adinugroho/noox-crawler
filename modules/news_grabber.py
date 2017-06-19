import re
import requests
import pymysql
import hashlib
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

    __verboseprint = None

    def __init__(self, config, debug=False, verbose=False):
        self._config = config
        self._is_debug = debug
        self._is_verbose = verbose
        self.__verboseprint = print if self._is_verbose or self._is_debug else lambda *a, **k: None

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

    def process(self, url_list, url_check_callback=None):
        """
        Extract the content of the pages from given urls.
        :param url_list list: url to extract
        :param url_check_callback function: callback to remove url from list
        :rtype: list
        """
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
            while urls and len(buffer_) < 20:
                # fill the buffer to 10 (pass by reference)
                self._fill_buffer(buffer_, urls)

                if callable(url_check_callback):
                    # retrieve urls already in database
                    toRemove = url_check_callback(buffer_)
                    # we select url that is not in database yet
                    buffer_ = [url for url in buffer_ if url not in toRemove]

            # after the buffer is full and the news is not in the database, retrieve the data
            for url in buffer_:
                self.__verboseprint('Extracting: "{0}"'.format(url))
                try:
                    req_data = requests.get(url, self._header)
                except Exception as e:
                    continue

                tags = []
                tags.append(self._config["title_tag"])
                tags.append(self._config["date_tag"])
                tags.append(self._config["article_tag"])
                tags.append(self._config["author_tag"])
                if not (self._config['img_wrapper_tag'] is None or len(self._config['img_wrapper_tag']) < 1):
                    tags.append(self._config['img_wrapper_tag'])
                tags.append("img")
                soup = BeautifulSoup(req_data.text, 'lxml', parse_only=SoupStrainer(tags))

                if "title_attr" in self._config and self._config["title_attr"] is not None:
                    title = soup.find(self._config["title_tag"], {self._config["title_attr"]: re.compile(self._config["title_attr_val"])}).get_text()
                else:
                    title = soup.find(self._config["title_tag"]).get_text()
                if title is None or len(title) < 5:
                    print('url "{0}" title is "{1}"'.format(url, title))
                    if self._is_debug:
                        raise RuntimeError('DEBUG: Title extraction error.')
                    continue

                try:
                    if "date_attr" in self._config and self._config["date_attr"] is not None:
                        date = soup.find(self._config["date_tag"], {self._config["date_attr"]: re.compile(self._config["date_attr_val"])}).get_text()
                    else:
                        date = soup.find(self._config["date_tag"]).get_text()
                    sqlDate = self._date_parser(date)
                    if sqlDate is None:
                        print('url "{0}" date is "{1}"'.format(url, sqlDate))
                        if self._is_debug:
                            raise RuntimeError('DEBUG: Date extraction error.')
                        continue
                except Exception as e:
                    print('unable to scan "{0}" because "{1}"'.format(url, str(e)))
                    if self._is_debug:
                        raise RuntimeError('DEBUG: Date extraction error.')
                    continue

                # if prioritize wrapper or img_tag_attr is empty or null, use wrapper immediately
                if self._config['img_tag_src_attr'] is not None and len(self._config['img_tag_src_attr']) > 0:
                    src_attr = self._config['img_tag_src_attr']
                else:
                    src_attr = 'src'

                img_url = None
                if self._config['prioritize_wrapper'] or (self._config['img_tag_attr'] is None or len(self._config['img_tag_attr']) < 1):
                    if "img_wrapper_attr" in self._config and self._config["img_wrapper_attr"] is not None:
                        wrapper = soup.find(self._config["img_wrapper_tag"], {self._config["img_wrapper_attr"]: re.compile(self._config["img_wrapper_attr_val"])})
                    else:
                        wrapper = soup.find(self._config["img_wrapper_tag"])

                    if wrapper is not None:
                        img_tags = wrapper.find_all('img')

                    if img_tags is not None:
                        for img_tag in img_tags:
                            if img_tag.has_attr(src_attr):
                                img_url = img_tag[src_attr]
                                break
                else:
                    img_tag = soup.find('img', {self._config["img_tag_attr"]: re.compile(self._config["img_tag_attr_val"])})
                    if img_tag is not None and img_tag.has_attr(src_attr):
                        img_url = img_tag[src_attr]
                if img_url is None or len(img_url) < 5:
                    if self._config['allow_no_image']:
                        self.__verboseprint('No img_url for {0}'.format(url))
                        img_url = None
                    else:
                        print('Skipping to scan "{0}" because no img_url'.format(url))
                        if self._is_debug:
                            raise RuntimeError('DEBUG: Image url extraction error.')
                        continue

                try:
                    if "author_attr" in self._config and self._config["author_attr"] is not None:
                        author = soup.find(self._config["author_tag"], {self._config["author_attr"]: re.compile(self._config["author_attr_val"])}).get_text()
                    else:
                        author = soup.find(self._config["author_tag"]).get_text()
                    if author is None or len(author) < 5:
                        if self._config['allow_default_author']:
                            self.__verboseprint('Using default author name...')
                            author = self._config['default_author_name']
                        else:
                            print('url "{0}" author is "{1}"'.format(url, author))
                            continue
                    if 'author_regex' in self._config and len(self._config['author_regex']) > 0:
                        author_regex = re.compile(self._config['author_regex'])
                        author = author_regex.search(author).group(1)
                        author = author.title()
                except Exception as e:
                    if self._config['allow_default_author']:
                        self.__verboseprint('Using default author name...')
                        author = self._config['default_author_name']
                    else:
                        print('unable to scan "{0}" because author is: "{1}"'.format(url, str(e)))
                        if self._is_debug:
                            raise RuntimeError('DEBUG: Author extraction error.')
                        continue

                if "article_attr" in self._config:
                    article_parts = soup.find_all(self._config["article_tag"], {self._config["article_attr"]: re.compile(self._config["article_attr_val"])})
                else:
                    article_parts = soup.find_all(self._config["article_tag"])

                if "article_bs_remove" in self._config and len(self._config["article_bs_remove"]) > 0:
                    for i, item in enumerate(article_parts):
                        for def_ in self._config["article_bs_remove"]:
                            if 'attr' in def_:
                                if len(def_['attr_val']) < 1:
                                    raise ValueError('attr_val is empty')
                                elements = article_parts[i].find_all(def_['tag'], {def_['attr']: re.compile(def_['attr_val'])})
                            else:
                                elements = article_parts[i].find_all(def_['tag'])
                            for el in elements:
                                el.decompose()

                article = ''
                for part in article_parts:
                    cleantext = re.sub(self.__compiled_regex, '', str(part))

                    if "article_tag_replace" in self._config:
                        article += self._multireplace(cleantext, self._config["article_tag_replace"])
                    else:
                        article += self._multireplace(cleantext, spc_chars)
                article = re.sub(r"(?:<br\/?>){3,}", '<br><br>', article)
                if article is None or len(article) < 200:
                    print('url "{0}" content is "{1}"'.format(url, article))
                    continue
                # print(article)
                ret.append({'title': title, 'url': url, 'author': author, 'pubtime': sqlDate, 'content': article, 'img_url': img_url})
                # print({'title': title, 'text': article})
        return ret

    def _fill_buffer(self, buffer_, urls):
        """
        Fill buffer from pool of urls.
        :param buffer_ list: buffer to fill
        :param urls deque: pool of urls
        """
        while urls and len(buffer_) < 20:
            url = urls.popleft()
            if self._config['sitename'] == self._get_domain_name(url):
                buffer_ += [url]

    def _date_parser(self, date):
        """
        Attempt to parse a date from a given string.
        :param date str: date to parse
        :rtype: str
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
            'agu': '08',
            'sep': '09',
            'oct': '10',
            'okt': '10',
            'nov': '11',
            'dec': '12',
            'des': '12'
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
            dateObj = parser.parse(date, dayfirst=True)
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
