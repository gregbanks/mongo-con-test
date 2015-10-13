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

from __future__ import print_function, division

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


_CONNECTION_STR = ''
_DONE = False
_INC_CLIENTS = False
_DEC_CLIENTS = False
_COL_BASE = 'foo'
_COL_NUM = 10
_INDEXES_CREATED = {}


def client(id_, uri, rate, stop_event):
    name = '{}-client{}'.format(socket.getfqdn(), id_)

    logging.info('%s starting up', name)

    c = MongoClient(uri)
    db = c.get_default_database()
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

    while not stop_event.wait(time_to_sleep):
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

    stop_event = Event()

    logging.info('spawning clients...')

    clients = []
    step = int(opts['--step'])

    try:
        for i in xrange(int(opts['--con-num'])):
            logging.debug('spawning client %d...', i)
            clients.append(gevent.spawn(client, i, _CONNECTION_STR,
                           opts['--rate'], stop_event))

        while not _DONE:
            global _INC_CLIENTS, _DEC_CLIENTS
            if _DEC_CLIENTS:
                num_clients = len(clients)
                for i in xrange(num_clients - 1,
                                max(-1, num_clients - 1 - step),
                                -1):
                    logging.debug('killing client %d...', i)
                    c = clients.pop()
                    c.kill()
                _DEC_CLIENTS = False
            if _INC_CLIENTS:
                num_clients = len(clients)
                for i in xrange(step):
                    logging.debug('spawning client %d...', num_clients + i)
                    clients.append(
                        gevent.spawn(client, num_clients + i, _CONNECTION_STR,
                                     opts['--rate'], stop_event))
                _INC_CLIENTS = False
            gevent.sleep(.25)

        logging.info('signaling clients to exit...')
        stop_event.set()

        logging.info('waiting on clients...')
        for c in clients:
            c.join()
    except Exception as e:
        logging.exception('error running contest')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

