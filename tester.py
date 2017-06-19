import os
import json
import hashlib
import pymysql
import argparse
import re
from modules import LinkExtractor, NewsGrabber
from output_providers import NooxSqlProvider, JsonProvider

o_providers = {
    'json': JsonProvider('liputan6.json', True),
    'NooxDB': NooxSqlProvider({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdbapi'})
}

alnum_re = re.compile(r'^[A-Za-z0-9]{3,}$')


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


def parse_args():
    parser = argparse.ArgumentParser(description='Noox crawler driver.')
    parser.set_defaults(debug=False, verbose=False)
    parser.add_argument('--debug', action='store_true', help='exit immediately when error occured')
    parser.add_argument('-v', '--verbose', action='store_true', help='message verbosity')
    parser.add_argument('-o', '--output', action='append', type=str, help='output providers (default = json), can be multiple (-o json -o other)')
    parser.add_argument('target', help='site to be scanned (enter "all" to scan all sites)')
    return parser.parse_args()


def main():
    args = parse_args()

    o_destinations = []
    if args.output is None:
        o_destinations = o_providers['json']
    else:
        for dest in args.output:
            if dest not in o_providers:
                raise ImportError('Provider for {0} not found.'.format(dest))
            o_destinations.append(o_providers[dest])

    if args.target == 'all':
        pass
    elif alnum_re.match(args.target):
        conf_dir = './config/{0}.conf.json'.format(args.target)
        if os.path.isfile(conf_dir):
            with open(conf_dir) as conf_file:
                config = json.load(conf_file)
                # print(config)
                a = LinkExtractor(config, debug=args.debug, verbose=args.verbose)
                # print(a.get_urls(1))
                links = a.get_urls(max_link=100)
                grabber = NewsGrabber(config, debug=args.debug, verbose=args.verbose)
                news = grabber.process(links[:10], url_check_callback=check_with_db)
                print('Scanned '+str(len(news))+' out of '+str(len(links))+' links...')

                file = JsonProvider('liputan6.json', True, news)
                print('Writing to file...')
                file.save()

                out = NooxSqlProvider({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdbapi'}, config['noox_config'], news)
                # print('Saving to database...')
                ids = out.save()
                print(ids)
        else:
            raise OSError(2, 'Configuration file not found', './config/{0}.conf.json'.format(args.target))

if __name__ == '__main__':
    main()