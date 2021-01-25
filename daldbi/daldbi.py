# -*- coding: utf-8 -*-

from pydal.objects import Table, Field
from pydal import adapters

migrate = False

class DbiAdapter:
    datatypes = {}
    def __init__(self, db, schema='public') -> None:
        super().__init__()
        self.db = db

    def get_fields(self, table) -> list:
        raise Exception('Not implemented')
        return []

    def get_tables(self) -> list:
        raise Exception('Not implemented')
        return []


class DbiSqlite(DbiAdapter):
    adapter = adapters.sqlite.SQLite
    datatypes = {
        'TEXT': 'text',
        'NUMERIC': 'decimal(10,2)',
        'NUM': 'decimal(10,2)',
        'INTEGER': 'integer',
        'INT': 'integer',
        'REAL': 'decimal(10,2)',
    }
    
    def get_fields(self, table) -> list:
        sql = "PRAGMA table_info('%s')" % table
        fields = self.db.executesql(sql, as_dict=True)
        print(fields)
        return [(f['name'], 'id' if f['pk'] == 1 else self.datatypes[f['type'].upper()]) for f in fields]     

    def get_tables(self) -> list:
        sql = """SELECT name as table_name FROM sqlite_master 
            WHERE type in ('table', 'view') AND 
            NOT name LIKE('sqlite_%');"""

        rows = self.db.executesql(sql, as_dict=True)
        return sorted([r['table_name'] for r in rows])

class DbiPostgresql(DbiAdapter):
    adapter = adapters.postgres.Postgre
    datatypes = {
        'varchar': 'string',
        'int': 'integer',
        'boolean': 'boolean',
        'integer': 'integer',
        'tinyint': 'integer',
        'smallint': 'integer',
        'mediumint': 'integer',
        'bigint': 'integer',
        'float': 'double',
        'double': 'double',
        'char': 'string',
        'character': 'string',
        'decimal': 'integer',
        'date': 'date',
        'time': 'time',
        'timestamp': 'datetime',
        'datetime': 'datetime',
        'interval': 'string',
        'binary': 'blob',
        'blob': 'blob',
        'tinyblob': 'blob',
        'mediumblob': 'blob',
        'longblob': 'blob',
        'text': 'text',
        'tinytext': 'text',
        'mediumtext': 'text',
        'longtext': 'text',
        'bit': 'boolean',
        'nvarchar': 'text',
        'numeric': 'decimal(30,15)',
        'real': 'decimal(30,15)',
        'character varying': 'string',
        'timestamp without time zone': 'datetime',
        'time without time zone': 'string',
        'double precision': 'double'
    }
    def __init__(self, db, schema='public') -> None:
        super().__init__(db, schema=schema)
        self.schema = schema or 'public'

    def get_fields(self, table) -> list:
        if table.find('.') > -1:
            self.schema, table = table.split('.')      

        sql = """SELECT column_name, data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision, numeric_precision_radix, numeric_scale,
            column_default
            FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name='%s' 
            ORDER BY ordinal_position""" % (self.schema, table)

        fields = self.db.executesql(sql, as_dict=True)
        res = []
        for f in fields:
            if (('column_default' in f) and 
                f['column_default'] and 
                f['column_default'].startswith('nextval')):
                res += [(f['column_name'], 'id')]
            elif f['data_type'] == 'numeric':
                res += [(f['column_name'], 'decimal(%(numeric_precision)d, %(numeric_scale)d)' % f)]
            elif f['data_type'] != 'USER-DEFINED':
                res += [(f['column_name'], self.datatypes[f['data_type']])]
        return res 

    def get_tables(self) -> list:
        sql = """SELECT table_name FROM information_schema.tables 
            WHERE table_schema = '%s' AND table_name LIKE '%%'AND
            NOT table_name LIKE('pg_%%');"""
        rows = self.db.executesql(sql % self.schema, as_dict=True)
        return sorted([row['table_name'] for row in rows])

class DbiAdapters:
    dbi_adapters = [DbiSqlite, DbiPostgresql]
    @classmethod
    def get_adapter(cls, db, schema):
        for adpt in cls.dbi_adapters:
            if isinstance(db._adapter, adpt.adapter):
                print(adpt)
                return adpt(db, schema=schema)
        raise Exception('Adapter %s not found'.format(db._adapter.__class__))

class DbInspector(object):
    def __init__(self, db, schema=None, auto_define=False):
        self.adapter = DbiAdapters.get_adapter(db, schema=schema)
        self.schema = self.adapter.schema
        self.db = db
        self.tables = []
        if auto_define:
            for t in self.adapter.get_tables():
                self._define_table("{0}.{1}".format(self.schema, t))
        else:
            self.tables = self.adapter.get_tables()

    def table_def(self, table, with_id=False):

        tpl = """db.define_table('%(tablename)s',%(sep)s%(fields)s%(sep)s%(sep)srname='%(rname)s',migrate=migrate)"""
        
        cond = lambda x: True if with_id else x != 'id'
        flt_flds = ((f, t) for f, t in self.adapter.get_fields(table) if cond(f) )

        params = {}
        params['sep'] = '\n\t\t'

        fields = params['sep'].join(["Field('%s', type='%s')," %(f, t) for f, t in flt_flds ])
        if self.schema and self.schema != 'public': 
            params.update({'tablename': "%s_%s" % (self.schema, table), 'fields': fields})
        else:
            params.update({'tablename': table, 'fields': fields})

        if self.schema:
            params['rname']= "%s.%s" % (self.schema, table)
        else:
            params['rname'] = table
        return tpl % params
    
    def _define_table(self, table):
        db = self.db
        #if table.find('.') > -1:
        #    self.schema, table = table.split('.')        
        code = self.table_def(table)
        exec(code)
        self.tables.append(table)
        return table