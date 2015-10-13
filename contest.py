"""MongoDB connection test

Usage:
    contest.py [options] [CONNECTION_STR]

Opts:
    -h --help               print this help
    -v --verbose            log debug output
    -r --rate RATE          rate at which to perform writes/reads (OPS/s) [default: .1]
    -n --con-num NUM        the number of connections to start up [default: 50]
    -s --step NUM           the number of connections to increase or decrease
                            the pool by when receiving SIGUSER1 (-) or SIGUSER2 (+)
                            [default: 50]

Args:
    CONNECTION_STR          the connection string (a default hard-coded value
                            will be used if this is absent)

"""
from gevent import monkey; monkey.patch_all()

import logging
import os
import random
import socket
import sys
import signal

from uuid import uuid1

import docopt
import gevent

from gevent.event import Event
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


_CONNECTION_STR = ''
_DONE = False
_INC_CLIENTS = False
_DEC_CLIENTS = False
_COL_BASE = 'foo'
_COL_NUM = 10
_SPAWN_WAIT = .01
_INDEXES_CREATED = {}


def client(id_, uri, rate):
    name = '{}-client{}'.format(socket.getfqdn(), id_)

    c = MongoClient(uri)

    """
    while True:
        try:
            c.admin.command('ping')
        except ServerSelectionTimeoutError:
            time_to_sleep = random.uniform(0, 10)
            logging.debug('%s failed to select server, sleeping %f seconds...',
                          name, time_to_sleep)
            gevent.sleep(time_to_sleep)
            continue
        break
    """

    db = c.get_default_database()

    logging.info('%s connected', name)

    col = '{}{}'.format(_COL_BASE, id_ % _COL_NUM)
    if col in _INDEXES_CREATED:
        _INDEXES_CREATED[col].wait()
    else:
        _INDEXES_CREATED[col] = Event()
        db[col].create_index('data')
        _INDEXES_CREATED[col].set()

    delta = 1 / rate
    tolerance = delta / 10
    time_to_sleep = random.uniform(0, delta) # stagger startup

    while not gevent.sleep(time_to_sleep):
        try:
            uuid_ = uuid1()
            doc = {
                'name': name,
                'data': str(uuid_)
            }
            logging.debug('inserting %r', doc)
            db[col].insert(doc, w=1)
            logging.debug('reading %r', doc)
            curs = db[col].find({'data': str(uuid_)})
            assert(curs.count() == 1)
            logging.debug('read %r', curs.next())
            curs.close()
        except Exception as e:
            logging.error('%s encountered an error on write/read: %r', name, e)
        time_to_sleep = delta + random.uniform(-tolerance, tolerance)
        logging.debug('%s going to sleep for %fs...', name, time_to_sleep)
    logging.info('%s exiting...', name)


def sig_handler(num, frame):
    global _DONE, _INC_CLIENTS, _DEC_CLIENTS
    if num == signal.SIGINT:
        _DONE = True
    if num == signal.SIGUSR1:
        _DEC_CLIENTS = True
    if num == signal.SIGUSR2:
        _INC_CLIENTS = True


def main():
    opts = docopt.docopt(__doc__)
    opts['--rate'] = float(opts['--rate'])

    global _CONNECTION_STR
    _CONNECTION_STR = opts['CONNECTION_STR'] if opts['CONNECTION_STR'] \
                                             else _CONNECTION_STR

    logging.basicConfig(level=logging.DEBUG if opts['--verbose']
                                            else logging.INFO)

    try:
        c = MongoClient(_CONNECTION_STR)
    except Exception as e:
        logging.exception('error connection to %s', _CONNECTION_STR)
        return 1

    c.close()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGUSR1, sig_handler)
    signal.signal(signal.SIGUSR2, sig_handler)

    logging.info('spawning clients...')

    clients = []
    step = int(opts['--step'])

    def start_clients(num_clients):
        current_num_clients = len(clients)
        for i in xrange(num_clients):
            if _DONE:
                break
            logging.debug('spawning client %d...', current_num_clients + i)
            clients.append(gevent.spawn(client, current_num_clients + i, _CONNECTION_STR,
                           opts['--rate']))
            gevent.sleep(_SPAWN_WAIT)

    def stop_clients(num_clients=None):
        if num_clients is None:
            num_clients = len(clients)
        current_num_clients = len(clients)
        for i in xrange(current_num_clients - 1,
                        max(-1, current_num_clients - 1 - num_clients),
                        -1):
            logging.debug('killing client %d...', i)
            c = clients.pop()
            c.kill()

    try:
        start_clients(int(opts['--con-num']))
        while not _DONE:
            global _INC_CLIENTS, _DEC_CLIENTS
            if _DEC_CLIENTS:
                num_clients = len(clients)
                _DEC_CLIENTS = False
            if _INC_CLIENTS:
                start_clients(step)
                _INC_CLIENTS = False
            logging.debug('main thread sleeping...')
            gevent.sleep(2)

        logging.info('killing all clients...')
        stop_clients()
    except Exception as e:
        logging.exception('error running contest')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

