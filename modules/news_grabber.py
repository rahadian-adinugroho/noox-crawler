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
        self.__cur_url = None
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
            for self.__cur_url in buffer_:
                self.__verboseprint('Extracting: "{0}"'.format(self.__cur_url))
                try:
                    req_data = requests.get(self.__cur_url, self._header)
                except Exception as e:
                    continue

                # find tag
                soup = BeautifulSoup(req_data.text, 'lxml', parse_only=SoupStrainer(self._find_item(self._config['to_extract'], 'tag')))

                data = self.extract_soup(soup, self._config['to_extract'])

                if data == 61:
                    if self._is_debug:
                        raise ValueError('[DEBUG] returned data is {0}, expected dict type'.format(data))
                    else:
                        continue
                data.update({'url': self.__cur_url})
                ret.append(data)
        return ret

    def extract_soup(self, soup, config):
        data = {}
        for el in config:
            if el == 'save':
                if isinstance(config[el], list):
                    for toSave in config[el]:
                        contents = self._get_content(soup, toSave)
                        if contents == 61:
                            print('[WARNING] url: "{0}" does not have required element: "{1}"'.format(self.__cur_url, toSave['as']))
                            return 61
                        if contents is None:
                            self.__verboseprint('[INFO] url: "{0}", element: "{1}" value is none'.format(self.__cur_url, toSave['as']))
                        data.update({toSave['as']: contents})
                if isinstance(config[el], dict):
                    contents = self._get_content(soup, config[el])
                    if contents == 61:
                        print('[WARNING] url: "{0}" does not have required element: "{1}"'.format(self.__cur_url, config[el]['as']))
                        return 61
                    if contents is None:
                        self.__verboseprint('[INFO] url: "{0}", element: "{1}" value is none'.format(self.__cur_url, config[el]['as']))
                    data.update({config[el]['as']: contents})
            elif 'container' in el:
                if not isinstance(config[el], dict):
                    raise TypeError('In key: "{0}"; key content is expected to be dict type. (*container* is considered as wrapper tag)')

                if 'attr' in config[el] and config[el]['attr'] is not None:
                    bsTag = soup.find(config[el]["tag"], {config[el]["attr"]: re.compile(config[el]["attr_val"])})
                else:
                    bsTag = soup.find(config[el]["tag"])

                datas = self.extract_soup(bsTag, config[el])
                if datas == 61:
                    return 61
                data.update(datas)
        return data

    def _get_content(self, bsTag, config):
        ret = None
        if bsTag is None:
            if 'required' not in config or config['required']:
                # the container does not exist in the first place
                # ENODATA value
                print('[ERROR] container does not exist')
                return 61
            else:
                return None
        if isinstance(config, dict):
            # find the tag from the soup
            if 'attr' in config and config['attr'] is not None:
                tag = bsTag.find(config["tag"], {config["attr"]: re.compile(config["attr_val"])})
            else:
                tag = bsTag.find(config["tag"])

            if 'save_attr' in config:
                # save attribute value instead of tag content
                if tag is None or not tag.has_attr(config['save_attr']):
                    if 'required' not in config or config['required']:
                        # required is not defined or element is required
                        # ENODATA value
                        return 61
                    return ret
                if 'format' in config and config['format'] is not None:
                    ret = self._format_content(tag, config['format'], save_attr=config['save_attr'])
                else:
                    ret = tag[config['save_attr']]
            else:
                if tag is None:
                    if 'required' not in config or config['required']:
                        # ENODATA value
                        return 61
                    return ret
                if 'format' in config and config['format'] is not None:
                    ret = self._format_content(tag, config['format'])
                else:
                    ret = tag.get_text()
        else:
            raise TypeError('config parameter is expected to be type of dict')
        return ret

    def _format_content(self, bsTag, config, save_attr=None):
        if 'type' in config and config['type'] == 'title':
            return bsTag[save_attr].title() if isinstance(save_attr, str) else bsTag.get_text().title()
        elif 'type' in config and config['type'] == 'date':
            return self._date_parser(bsTag[save_attr] if isinstance(save_attr, str) else bsTag.get_text(), config)
        elif 'type' in config and config['type'] == 'get_text':
            return bsTag.get_text()

        if "bs_remove" in config and len(config["bs_remove"]) > 0:
            for def_ in config["bs_remove"]:
                if 'attr' in def_:
                    if len(def_['attr_val']) < 1:
                        raise ValueError('attr_val is empty')
                    elements = bsTag.find_all(def_['tag'], {def_['attr']: re.compile(def_['attr_val'])})
                else:
                    elements = bsTag.find_all(def_['tag'])
                for el in elements:
                    el.decompose()

        if "regex_capture" in config and len(config["regex_capture"]) > 0:
            text = bsTag[save_attr] if isinstance(save_attr, str) else bsTag.get_text()
            cap_regex = re.compile(config["regex_capture"])
            cap = cap_regex.search(text).group(1)
            return cap if "regex_capture_title" not in config or not config['regex_capture_title'] else cap.title()

        if 'regex_remove' in config or 'replace' in config:
            regex = r'(?:<script(?:\s|\S)*?<\/script>)|(?:<style(?:\s|\S)*?<\/style>)|(?:<!--(?:\s|\S)*?-->)'
            # if regex to remove tag is not empty, add it to the base regex with | (or) separator.
            if "regex_remove" in config:
                regex += '|'+'|'.join(config["regex_remove"])

            # if regex to replace tag is not empty, add it to the exception list.
            if "replace" in config:
                config["replace"].update(self._spc_chars)
                regex += '|'+''.join(map(lambda tag: '(?!'+re.escape(tag)+')', config["replace"]))+r'(?:<\/?(?:\s|\S)*?>)'
            else:
                regex += '|'+r'(<\/?(\s|\S)*?>)'
            fin_regex = re.compile(regex)

            cleantext = re.sub(fin_regex, '', str(bsTag))

            if "replace" in config:
                fin_text = self._multireplace(cleantext, config["replace"])
            else:
                fin_text = self._multireplace(cleantext, spc_chars)

            fin_text = re.sub(r"(?:<br\/?>){3,}", '<br><br>', fin_text)
            if 'type' in config and config['type'] == 'article' and len(fin_text) < 400:
                print('[WARNING] url: "{0}" article is less than 250 characters'.format(self.__cur_url))
                return 61
            return fin_text
        return str(bsTag)

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

    def _date_parser(self, date, config):
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
        if 'normalize_date' not in config or config["normalize_date"]:
            repl = {}
            repl.update(month)
            repl.update(shortMonth)
            repl.update(bulan)
            date = self._multireplace(date.lower(), repl)
        if "date_regex" in config and len(config["date_regex"]) > 0:
            reg = re.compile(config["date_regex"])
            try:
                matches = reg.search(date).groupdict()
                date = '{d}/{m}/{y} {h}:{i}'.format_map(matches)
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

    def _find_item(self, obj, key):
        ret = []
        if isinstance(obj, dict):
            if key in obj:
                ret.append(obj[key])
            for i, d in obj.items():
                item = self._find_item(d, key)
                if len(item) > 0:
                    ret = [v for v in item if v not in ret] + ret
            return ret
        elif isinstance(obj, list):
            for d in obj:
                item = self._find_item(d, key)
                if len(item) > 0:
                    ret = [v for v in item if v not in ret] + ret
            return ret
        else:
            return ret

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

if __name__ == '__main__':
    conf = {
                "save": [
                    {
                        "tag": "meta",
                        "attr": "property",
                        "attr_val": "og:title",
                        "save_attr": "content",
                        "as": "title",
                    },
                ],
                "ctn_container":
                {
                    "tag": "div",
                    "attr": "id",
                    "attr_val": "news",
                    "save":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "detail_text|text_detail",
                        "as": "article",
                        "format":
                        {
                            "type": "article",
                            "bs_remove": [
                                {
                                    "tag": "div",
                                    "attr": "class",
                                    "attr_val": "box_hl wpgal |boxlr mt15"
                                }
                            ],
                            "replace":
                            {
                                "<br>": "<br>",
                                "<br/>": "<br/>",
                                "<strong>": "<strong>",
                                "</strong>": "</strong>",
                                "<b>": "<b>",
                                "</b>": "</b>",
                                "<em>": "<em>",
                                "</em>": "</em>"
                            },
                            "regex_remove":
                            [
                                "(?:<table.*?<\\/table>)",
                                "(?:\\[.+?Video.*?\\])"
                            ],
                        }
                    },
                    "loc_container":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "detail_text",
                        "save":
                            {
                                "tag": "strong",
                                "attr": None,
                                "attr_val": None,
                                "as": "loc"
                            }
                    },
                    "jdl_container":
                    {
                        "tag": "div",
                        "attr": "class",
                        "attr_val": "jdl",
                        "save": [
                            {
                                "tag": "span",
                                "attr": 'class',
                                "attr_val": 'author',
                                "as": "author",
                                "format":
                                {
                                    "regex_capture": "((?:[a-zA-Z]+\s)*[a-zA-Z]+)\s+-",
                                    "regex_capture_title": False
                                }
                            },
                            {
                                "tag": ["span", "div"],
                                "attr": 'class',
                                "attr_val": 'date',
                                "as": "date",
                                "format":
                                {
                                    "type": "date",
                                    "normalize_date": True
                                }
                            }
                        ]
                    },
                },
                "image_container":
                {
                    "tag": "div",
                    "attr": "class",
                    "attr_val": "pic_artikel|media_artikel",
                    "save": [
                        {
                            "tag": "img",
                            "attr": None,
                            "attr_val": None,
                            "save_attr": "src",
                            "as": "img_url",
                        }
                    ]
                }
            }
    ng = NewsGrabber({}, verbose=True)
    url = 'https://inet.detik.com/inetgrafis/d-3518600/wonder-woman-dan-deretan-superhero-dc-terlaris'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml', parse_only=SoupStrainer(ng._find_item(conf, 'tag')))
    print(ng.extract_soup(soup, conf))
