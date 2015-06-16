#! /usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
Bench mark (RethinkDB and MongoDB)
"""
__author__ = 'Yoshiya Ito <myon53@gmail.com>'
__version__ = '1.0.0'
__date__ = '16 Jun 2015'

import asyncio
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

    def initialize(self, db):
        self.db = db

    def get(self):
        pass


class SyncBenchRethinkDB(tornado.web.RequestHandler):

    def initialize(self, db):
        self.db = db

    def get(self):
        pass

class AsyncBenchMongoDB(tornado.web.RequestHandler):

    def initialize(self, db):
        self.db = db

    @tornado.gen.coroutine
    def get(self):
        pass


class AsyncBenchRethinkDB(tornado.web.RequestHandler):

    def initialize(self, db):
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
        ('/', Index),
        ('/sync/mongo', BenchMongoDB, db=mongo['test']),
        ('/sync/rethink', BenchRethinkDB, db=rethink),
        ('/async/mongo', BenchMongoDB, db=mongo['test']),
        ('/async/rethink', BenchRethinkDB, db=rethink)
    ])

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    asyncio.get_event_loop().run_forever()
