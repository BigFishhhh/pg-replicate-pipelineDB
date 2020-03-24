# coding: utf-8
# from __future__ import print_function

import json
from datetime import datetime

import psycopg2
import psycopg2.extras


from util import log


class PipelinedbRepliaction(object):
    """CRUD replication to pipelinedb"""

    def __init__(self, tables, plugin, username=None, password=None,  connection=None):
        self.db_tables = tables
        self.plugin = plugin
        self.exclude_columns = {}
        self.include_columns = {}
        self.filters = {}
        if connection:
            log.info('pipelinedb connection to %s ...' % connection)
            self.pipelinedb = psycopg2.connect(connection)
        else:
            log.info('pipelinedb connection to localhost pipelinedb')
            self.pipelinedb = psycopg2.connect(connection)

        self.table_ids = {}
        def init_values(table):
            log.info('Creating table %s ...' % table['name'])
            self.table_ids[table['name'].strip()] = table['primary_key']
            cur = self.pipelinedb.cursor()
            cur.execute('''select * from information_schema.tables where table_name='%s';''' % table['name'])
            tag = bool(cur.rowcount)
            if tag:
                log.info('%s already exists!' % table['name'])
                pass
            else:
                cur.execute('''CREATE FOREIGN TABLE %s ( id varchar, age varchar) SERVER pipelinedb;''' % table['name'])
                self.pipelinedb.commit()
                log.info('%s create successful!' % table['name'])
            # if 'exclude_columns' not in table:
            #     self.exclude_columns += table['exclude_columns'].split(',')
            if 'include_columns' in table:
                self.include_columns[table['name']] = table['include_columns'].split(',')
            self.filters[table['name']] = json.loads(json.dumps(table.get('filters', [])))

        map(init_values, tables)

    def handle_dates(self, document, column, value):
        try:
            # document[column] = parse(value)
            if column in ['created_at', 'delivery_at', 'order_time'] and self.plugin == 'decoderbufs':
                document[column] = datetime.utcfromtimestamp(int(value)/1000000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                document[column] = value
        except Exception as e:
            raise e
        return document

    def parse_doc_body(self, document, change):
        if self.plugin == 'wal2json':
            for f in self.filters[change['table']]:
                column = f['columns']
                before = f['before']
                after = f['after']
                after_column_index = change['columnnames'].index(column)
                after_value = change['columnvalues'][after_column_index]

                if 'oldkeys' in change and column in change['oldkeys']['keynames']:
                    before_column_index = change['oldkeys']['keynames'].index(column)
                    before_value = change['oldkeys']['keyvalues'][before_column_index]
                else:
                    return document
                if before != before_value or after != after_value:
                    return document

            for idx, column in enumerate(change['columnnames']):
                if column == document['id']:
                    document['id'] = change['columnvalues'][idx]
                    if type(document['id']) == str or type(document['id']) == unicode:
                        document['id'] = document['id'].strip()
                elif column in self.include_columns[change['table']]:
                    document = self.handle_dates(document, change['columnnames'][idx], change['columnvalues'][idx])
                # elif column not in self.exclude_columns:
                #     document = self.handle_dates(document, change['columnnames'][idx], change['columnvalues'][idx])
            return document
        elif self.plugin == 'decoderbufs':
            table_name = change['table'].replace('public.', '')
            if table_name == 'nt_salesman_performance' and change['op'] == 'UPDATE':
                before_salesman_performance = 0
                before_quantity = 0
                for ot in change.get('oldTuple', []):
                    if ot.get('columnName') == 'active_salesman_performance':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                before_salesman_performance = v
                    elif ot.get('columnName') == 'quantity':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                before_quantity = v

                for ot in change.get('newTuple', []):
                    if ot.get('columnName') == document['id']:
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                document['id'] = v
                        if type(document['id']) == str or type(document['id']) == unicode:
                            document['id'] = document['id'].strip()
                    elif ot.get('columnName') == 'active_salesman_performance':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                document['active_salesman_performance'] = float(v) - float(before_salesman_performance)
                    elif ot.get('columnName') == 'quantity':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                document['quantity'] = float(v) - float(before_quantity)
                    elif ot.get('columnName') == 'order_time':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                document['order_time'] = datetime.utcfromtimestamp(int(v)/1000000).strftime("%Y-%m-%d %H:%M:%S")
                    elif ot.get('columnName') == 'salesman_id':
                        for k, v in ot.items():
                            if k.startswith('datum'):
                                document['salesman_id'] = v
                return document
            for f in self.filters[table_name]:
                column = f['columns']
                before = f['before']
                after = f['after']
                after_value = None
                before_value = None
                if 'oldTuple' in change:
                    for ot in change.get('oldTuple', []):
                        if ot.get('columnName') == column:
                            for k, v in ot.items():
                                if k.startswith('datum'):
                                    before_value = v
                    for ot in change.get('newTuple', []):
                        if ot.get('columnName') == column:
                            for k, v in ot.items():
                                if k.startswith('datum'):
                                    after_value = v
                else:
                    return document
                if before != before_value or after != after_value:
                    return document

            for nt in change.get('newTuple', []):
                if nt.get('columnName') == document['id']:
                    for k, v in nt.items():
                        if k.startswith('datum'):
                            document['id'] = v
                    if type(document['id']) == str or type(document['id']) == unicode:
                        document['id'] = document['id'].strip()
                elif nt.get('columnName') in self.include_columns[table_name]:
                    for k, v in nt.items():
                        if k.startswith('datum'):
                            document = self.handle_dates(document, nt.get('columnName'), v)

            return document
        else:
            raise "don't support plugin: %s" % self.plugin

    def parse_insert_or_update(self, document, change):
        document = self.parse_doc_body(document, change)
        return document

    def replicate(self, data, initial=False, initial_table=None):

        def initial_replicate(entry):
            document = {}
            document['id'] = entry[self.table_ids[initial_table]]
            entry = dict(entry)

            for key, value in entry.iteritems():
                if key not in self.exclude_columns and key != document['id']:
                    document[key] = value
            return document

        def wal2json_normal_replicate(change):
            kind = change['kind']
            table = change['table']

            document = {}

            if kind in ['delete', 'insert', 'update'] and table in self.table_ids.keys():
                document['id'] = self.table_ids[table]
                document['table_name'] = table
                if kind == 'delete':
                    pass
                else:
                    document = self.parse_insert_or_update(document, change)

            else:
                pass
            return document

        def probuf_normal_replicate(change):
            kind = change['op']
            table = change['table'].replace('public.', '')
            document = {}

            if kind in ['DELETE', 'INSERT', 'UPDATE'] and table in self.table_ids.keys():
                document['id'] = self.table_ids[table]
                document['table_name'] = table
                if kind == 'DELETE':
                    pass
                else:
                    document = self.parse_insert_or_update(document, change)
            else:
                pass
            return document


        if initial and initial_table:

            """inital data，"inital_sync": true"""

            try:
                data_record = map(initial_replicate, data)
                self.pipelinedb.cursor().execute(
                    "INSERT INTO " + data['change'][0]['table'] + " (id,age) VALUES (%(id)s, %(age)s)", data_record)
                self.pipelinedb.commit()
            except Exception as e:
                log.error('抛出异常，异常信息为：%s' % e)
        else:
            if self.plugin == 'decoderbufs':
                data_to_replicate = [probuf_normal_replicate(data)]
            elif self.plugin == 'wal2json':
                data = data['change']
                data_to_replicate = map(wal2json_normal_replicate, data)
            else:
                raise "don't support plugin: %s" % self.plugin
            try:
                for record_data in data_to_replicate:
                    if len(record_data) > 2:
                        cols = record_data.keys()
                        cols.remove('table_name')
                        cols_value = ", ".join(cols)
                        vals = [record_data[x] for x in cols if x != 'table_name']
                        vals_str_list = ["%s"] * len(vals)
                        vals_str = ", ".join(vals_str_list)
                        log.info("INSERT INTO " + record_data['table_name'] + " ({cols}) VALUES ({vals_str})".format(
                            cols=cols_value, vals_str=vals))
                        self.pipelinedb.cursor().executemany("INSERT INTO " + record_data[
                            'table_name'] + '_streaming' + " ({cols}) VALUES ({vals_str})".format(
                            cols=cols_value, vals_str=vals_str), [vals])
                        self.pipelinedb.commit()
                        log.info('insert successful %s' % record_data)
                        log.info(record_data['table_name'])
            except Exception as e:
                log.error('抛出异常，异常信息为：%s' % e)
