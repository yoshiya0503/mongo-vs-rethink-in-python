#! /usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
Bench mark (RethinkDB and MongoDB)
"""
__author__ = 'Yoshiya Ito <myon53@gmail.com>'
__version__ = '1.0.0'
__date__ = '16 Jun 2015'

import asyncio
import time
import pymongo as m
import rethinkdb as r
from tornado.platform.asyncio import AsyncIOMainLoop
import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

define('port', default=8000, help='this is help', type=int)


class SyncBenchMongoDB(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    def get(self):
        return

    def bulk_insert_massive(self):
        start = time()
        bench = self.db.bench
        data = [{'_id': i, 'name': ('name' + str(i))} for i in range(0, 10000)]
        bench.insert_many(data)
        end = time()
        diff = end - start
        print('bulk_insert_massive in pymongo: ', str(diff))
        return diff

    def find_massive(self):
        start = time()
        bench = self.db.bench
        bench.find({})
        end = time()
        diff = end - start
        print('find_massive in pymongo: ', str(diff))
        return diff

    def find_one(self):
        start = time()
        bench = self.db.bench
        bench.find_one({'_id': 5555})
        end = time()
        diff = end - start
        print('find_one in pymongo: ', str(diff))
        return diff

    def delete_massive(self):
        start = time()
        bench = self.db.bench
        delete = {'_id': i for i in range(0, 10000)}
        bench.delete_many(delete)
        end = time()
        diff = end - start
        print('delete_massive in pymongo: ', str(diff))
        return diff


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
