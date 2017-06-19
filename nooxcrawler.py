import os
import glob
import json
import hashlib
import pymysql
import argparse
import re
from multiprocessing import Queue, Pool
from functools import partial
from modules import LinkExtractor, NewsGrabber
from output_providers import NooxSqlProvider, JsonProvider

o_providers = {
    'json': JsonProvider,
    'NooxDB': NooxSqlProvider
}


def check_with_db(urls):
    db = pymysql.connect('localhost', 'root', '', 'nooxdbapi')
    cursor = db.cursor()
    sql = 'SELECT `url` FROM `news` WHERE `url_hash` IN ({0})'
    in_p = ', '.join(map(lambda x: "'" + hashlib.md5(x.encode('utf-8')).hexdigest() + "'", urls))
    sql = sql.format(in_p)
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]


def parse_args():
    parser = argparse.ArgumentParser(description='Noox crawler driver.')
    parser.set_defaults(debug=False, verbose=False, limit=500)
    parser.add_argument('-o', '--output', action='append', type=str, help='output providers (default = json), can be multiple (-o json -o other)')
    parser.add_argument('-l', '--limit', type=int, help='limit links to be scanned')
    parser.add_argument('--debug', action='store_true', help='exit immediately when error occured')
    parser.add_argument('-v', '--verbose', action='store_true', help='message verbosity')
    parser.add_argument('target', help='site to be scanned (enter "all" to scan all sites)')
    return parser.parse_args()


def process_output_providers(destinations, config):
    o_destinations = []
    if destinations is None:
        o_destinations.append(o_providers['json'](config['sitename']+'.json', True))
    else:
        for dest in destinations:
            if dest not in o_providers:
                raise ImportError('Provider for {0} not found.'.format(dest))

            if dest == 'json':
                initialized = o_providers[dest](config['sitename']+'.json', True)
            elif dest == 'NooxDB':
                initialized = o_providers[dest]({'db_url': 'localhost', 'db_username': 'root', 'db_password': '', 'db_name': 'nooxdbapi'}, config['noox_config'])

            o_destinations.append(initialized)
    return o_destinations


def crawler(config: dict, args):
    verboseprint = print if args.verbose or args.debug else lambda *a, **k: None

    verboseprint('Scanning site: {0}'.format(config['sitename'].title()))

    verboseprint('Starting url scanning...')
    a = LinkExtractor(config, debug=args.debug, verbose=args.verbose)
    links = a.get_urls(max_link=args.limit)
    verboseprint('Found '+str(len(links))+' links...')
    verboseprint('Link extract finished...')

    verboseprint('Grabbing news data...')
    grabber = NewsGrabber(config, debug=args.debug, verbose=args.verbose)
    if args.limit > 0:
        news = grabber.process(links[:args.limit], url_check_callback=check_with_db)
    else:
        news = grabber.process(links, url_check_callback=check_with_db)
    verboseprint('Scanned '+str(len(news))+' out of '+str(len(links))+' links...')

    if len(news) > 0:
        for output in process_output_providers(args.output, config):
            verboseprint('Using output provider: {0}'.format(output.__class__.__name__))
            output.save(news)
    else:
        print('No data to output...')

    return


def main():
    args = parse_args()
    alnum_re = re.compile(r'^[A-Za-z0-9]{3,}$')

    partial_crawler = partial(crawler, args=args)
    if args.target == 'all':
        print('Scanning all sites in config folder...')
        configs = []
        for filename in glob.glob('./config/*.conf.json'):
            f = open(filename)
            configs.append(json.load(f))
        pool = Pool()
        pool.map(partial_crawler, configs)
        print('Operation finished...')
    elif alnum_re.match(args.target):
        print('Scanning site: {0}'.format(args.target.title()))
        conf_dir = './config/{0}.conf.json'.format(args.target)
        if os.path.isfile(conf_dir):
            with open(conf_dir) as conf_file:
                config = json.load(conf_file)
                partial_crawler(config)
                print('Operation finished...')
        else:
            raise OSError(2, 'Configuration file not found', './config/{0}.conf.json'.format(args.target))

if __name__ == '__main__':
    main()
