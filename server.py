#! /usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
Bench mark (RethinkDB and MongoDB)
"""
__author__ = 'Yoshiya Ito <myon53@gmail.com>'
__version__ = '1.0.0'
__date__ = '16 Jun 2015'

import asyncio
import json
import pymongo as m
import rethinkdb as r
import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.options import define, options
from contextlib import contextmanager
from time import time

define('port', default=8000, help='this is help', type=int)


class Chronos(object):

    def __init__(self, *, title='', start=None, end=None):
        '''
        args: start and end should not set time() as default
        '''
        self.title = title
        self.start = start
        self.end = end

    def duration(self):
        return (self.end - self.start)

    def __str__(self):
        return self.title + ': ' + str(self.duration())

    @contextmanager
    def __call__(self):
        try:
            self.start = time()
            yield
        finally:
            self.end = time()


class SyncBenchMongoDB(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    def get(self):
        time_bulk_insert_massive = self.bulk_insert_massive()
        time_find_massive = self.find_massive()
        time_find_one = self.find_one()
        time_delete_massive = self.delete_massive()
        res = {
            'time_bulk_insert_massive': time_bulk_insert_massive,
            'time_find_massive': time_find_massive,
            'time_find_one': time_find_one,
            'time_delete_massive': time_delete_massive
        }
        res_json = json.dumps(res)
        return self.write(res_json)

    def bulk_insert_massive(self):
        chronos = Chronos(title='bulk_insert_massive in pymongo')
        with chronos():
            bench = self.db.bench
            data = [{'_id': i, 'name': ('name' + str(i))} \
                    for i in range(0, 10000)]
            bench.insert_many(data)
        print(chronos)
        return chronos.duration()

    def find_massive(self):
        chronos = Chronos(title='find_massive in pymongo')
        with chronos():
            bench = self.db.bench
            _ = [doc for doc in bench.find({})]
        print(chronos)
        return chronos.duration()

    def find_one(self):
        chronos = Chronos(title='find_one in pymongo')
        with chronos():
            bench = self.db.bench
            bench.find_one({'_id': 5555})
        print(chronos)
        return chronos.duration()

    def delete_massive(self):
        chronos = Chronos(title='delete_massive in pymongo')
        with chronos():
            bench = self.db.bench
            bench.drop()
        print(chronos)
        return chronos.duration()


class SyncBenchRethinkDB(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    def get(self):
        pass


class AsyncBenchMongoDB(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    @tornado.gen.coroutine
    def get(self):
        pass


class AsyncBenchRethinkDB(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    @tornado.gen.coroutine
    def get(self):
        pass


if __name__ == '__main__':

    # We use event loop of asyncio instead of tornado IOLoop
    AsyncIOMainLoop().install()
    tornado.options.parse_command_line()

    mongo = m.MongoClient()
    rethink = r.connect(host='localhost', port=28015)

    app = tornado.web.Application(handlers=[
        ('/sync/mongo', SyncBenchMongoDB, dict(db=mongo['bench'])),
        ('/sync/rethink', SyncBenchRethinkDB, dict(db=rethink)),
        ('/async/mongo', AsyncBenchMongoDB, dict(db=mongo['bench'])),
        ('/async/rethink', AsyncBenchRethinkDB, dict(db=rethink))
    ])

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    asyncio.get_event_loop().run_forever()
