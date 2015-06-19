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
import motor as am
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


@contextmanager
def listen(server):
    pass


class Chronos(object):

    def __init__(self, *, title='', start=None, end=None):
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
        data = [{'_id': i, 'name': ('name' + str(i))} \
                for i in range(0, 10000)]
        with chronos():
            bench = self.db.bench
            bench.insert(data)
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
        try:
            r.db_create('bench').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Database already exist! skip operation...')
        else:
            print('Database exist...')
        self.db.use('bench')

        try:
            r.db('bench').table_create('bench').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Table already exist! skip operation...')
        else:
            print('Table exist...')

        try:
            r.table('bench').index_create('id').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Index already exist! skip operation...')
        else:
            print('index exist...')

    def get(self):
        time_insert_massive = self.insert_massive()
        time_get_massive = self.get_massive()
        time_filter = self.filter()
        time_delete_massive = self.delete_massive()
        res = {
            'time_insert_massive': time_insert_massive,
            'time_get_massive': time_get_massive,
            'time_filter': time_filter,
            'time_delete_massive': time_delete_massive
        }
        res_json = json.dumps(res)
        return self.write(res_json)

    def insert_massive(self):
        chronos = Chronos(title='insert_massive in rethink')
        data = [{'id': i, 'name': ('name' + str(i))} \
                for i in range(0, 10000)]
        with chronos():
            r.table('bench').insert(data).run(self.db)
        print(chronos)
        return chronos.duration()

    def get_massive(self):
        chronos = Chronos(title='get_massive in rethink')
        with chronos():
            _ = r.table('bench').between(0, 9999, index='id').run(self.db)
        print(chronos)
        return chronos.duration()

    def filter(self):
        chronos = Chronos(title='filter in rethink')
        with chronos():
            _ = r.table('bench').get(5555).run(self.db)
        print(chronos)
        return chronos.duration()

    def delete_massive(self):
        chronos = Chronos(title='delte_massive in rethink')
        with chronos():
            r.db('bench').table_drop('bench').run(self.db)
        print(chronos)
        return chronos.duration()


class AsyncBenchMotor(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    @tornado.gen.coroutine
    def get(self):
        time_bulk_insert_massive = yield self.bulk_insert_massive()
        time_find_massive = yield self.find_massive()
        time_find_one = yield self.find_one()
        time_delete_massive = yield self.delete_massive()
        res = {
            'time_bulk_insert_massive': time_bulk_insert_massive,
            'time_find_massive': time_find_massive,
            'time_find_one': time_find_one,
            'time_delete_massive': time_delete_massive
        }
        res_json = json.dumps(res)
        return self.write(res_json)

    @tornado.gen.coroutine
    def bulk_insert_massive(self):
        chronos = Chronos(title='bulk_insert_massive in pymongo')
        data = [{'_id': i, 'name': ('name' + str(i))} \
                for i in range(0, 10000)]
        with chronos():
            bench = self.db.bench
            yield bench.insert(data)
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def find_massive(self):
        chronos = Chronos(title='find_massive in pymongo')
        with chronos():
            bench = self.db.bench
            cursor = bench.find({})
            _ = [doc for doc in (yield cursor.to_list(length=10000))]
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def find_one(self):
        chronos = Chronos(title='find_one in pymongo')
        with chronos():
            bench = self.db.bench
            yield bench.find_one({'_id': 5555})
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def delete_massive(self):
        chronos = Chronos(title='delete_massive in pymongo')
        with chronos():
            bench = self.db.bench
            yield bench.drop()
        print(chronos)
        return chronos.duration()


class AsyncBenchMongo(tornado.web.RequestHandler):

    def initialize(self, *, db=None):
        self.db = db

    @tornado.gen.coroutine
    def get(self):
        time_bulk_insert_massive = yield from self.bulk_insert_massive()
        time_find_massive = yield from self.find_massive()
        time_find_one = yield from self.find_one()
        time_delete_massive = yield from self.delete_massive()
        res = {
            'time_bulk_insert_massive': time_bulk_insert_massive,
            'time_find_massive': time_find_massive,
            'time_find_one': time_find_one,
            'time_delete_massive': time_delete_massive
        }
        res_json = json.dumps(res)
        return self.write(res_json)

    @asyncio.coroutine
    def bulk_insert_massive(self):
        chronos = Chronos(title='bulk_insert_massive in pymongo')
        data = [{'_id': i, 'name': ('name' + str(i))} \
                for i in range(0, 10000)]
        context = asyncio.get_event_loop()
        with chronos():
            bench = self.db.bench
            yield from context.run_in_executor(None, bench.insert, data)
        print(chronos)
        return chronos.duration()

    @asyncio.coroutine
    def find_massive(self):
        chronos = Chronos(title='find_massive in pymongo')
        context = asyncio.get_event_loop()
        with chronos():
            bench = self.db.bench
            yield from context.run_in_executor(None, bench.find)
        print(chronos)
        return chronos.duration()

    @asyncio.coroutine
    def find_one(self):
        chronos = Chronos(title='find_one in pymongo')
        context = asyncio.get_event_loop()
        with chronos():
            bench = self.db.bench
            yield from context.run_in_executor(None, bench.find_one, {'_id': 5555})
        print(chronos)
        return chronos.duration()

    @asyncio.coroutine
    def delete_massive(self):
        chronos = Chronos(title='delete_massive in pymongo')
        context = asyncio.get_event_loop()
        with chronos():
            bench = self.db.bench
            yield from context.run_in_executor(None, bench.drop)
        print(chronos)
        return chronos.duration()



class AsyncBenchRethinkDB(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def initialize(self, *, db=None):
        self.db = db
        try:
            yield r.db_create('bench').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Database already exist! skip operation...')
        else:
            print('Database exist...')
        self.db.use('bench')

        try:
            yield r.db('bench').table_create('bench').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Table already exist! skip operation...')
        else:
            print('Table exist...')

        try:
            yield r.table('bench').index_create('id').run(self.db)
        except r.errors.RqlRuntimeError:
            print('Index already exist! skip operation...')
        else:
            print('index exist...')

    @tornado.gen.coroutine
    def get(self):
        time_insert_massive = yield self.insert_massive()
        time_get_massive = yield self.get_massive()
        time_filter = yield self.filter()
        time_delete_massive = yield self.delete_massive()
        res = {
            'time_insert_massive': time_insert_massive,
            'time_get_massive': time_get_massive,
            'time_filter': time_filter,
            'time_delete_massive': time_delete_massive
        }
        res_json = json.dumps(res)
        return self.write(res_json)

    @tornado.gen.coroutine
    def insert_massive(self):
        chronos = Chronos(title='insert_massive in rethink')
        data = [{'id': i, 'name': ('name' + str(i))} \
                for i in range(0, 10000)]
        with chronos():
            yield r.table('bench').insert(data).run(self.db)
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def get_massive(self):
        chronos = Chronos(title='get_massive in rethink')
        with chronos():
            cursor = yield r.table('bench').between(0, 9999, index='id').run(self.db)
            while (yield cursor.fetch_next()):
                _ = yield cursor.next()
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def filter(self):
        chronos = Chronos(title='filter in rethink')
        with chronos():
            _ = yield r.table('bench').get(5555).run(self.db)
        print(chronos)
        return chronos.duration()

    @tornado.gen.coroutine
    def delete_massive(self):
        chronos = Chronos(title='delte_massive in rethink')
        with chronos():
            yield r.db('bench').table_drop('bench').run(self.db)
        print(chronos)
        return chronos.duration()



if __name__ == '__main__':

    # We use event loop of asyncio instead of tornado IOLoop
    AsyncIOMainLoop().install()
    tornado.options.parse_command_line()

    mongo = m.MongoClient()
    rethink = r.connect(host='localhost', port=28015)
    motor = am.MotorClient()
    r.set_loop_type("tornado")
    rethink_torn = r.connect(host='localhost', port=28015)

    app = tornado.web.Application(handlers=[
        ('/sync/mongo', SyncBenchMongoDB, dict(db=mongo['bench'])),
        ('/sync/rethink', SyncBenchRethinkDB, dict(db=rethink)),
        ('/async/motor', AsyncBenchMotor, dict(db=motor['bench'])),
        ('/async/rethink', AsyncBenchRethinkDB, dict(db=rethink_torn)),
        ('/async/mongo', AsyncBenchMongo, dict(db=mongo['bench'])),
    ])

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt as e:
        print('server stoped...')
    finally:
        asyncio.get_event_loop().stop()
