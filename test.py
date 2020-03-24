# coding=utf-8
import json
import time

import psycopg2
import logging

logging.info('测试功能,测试表名：test_size')
with open('./pg2pipelinedb.json', 'r') as f:
    config = json.load(f)
psycopg2_connection_pipelinedb_string = 'dbname=%s host=%s user=%s password=%s port=%s' % (
            config['pipelinedb']['database'],
            config['pipelinedb']['host'],
            config['pipelinedb']['username'],
            config['pipelinedb']['password'],
            config['postgres']['port'],
        )
psycopg2_connection_string = 'dbname=%s host=%s user=%s password=%s port=%s' % (
            config['postgres']['database'],
            config['postgres']['host'],
            config['postgres']['username'],
            config['postgres']['password'],
            config['postgres']['port'],
        )
test_pipelinedb_conn = psycopg2.connect(psycopg2_connection_pipelinedb_string)
test_postgres_conn = psycopg2.connect(psycopg2_connection_string)

test_pipelinedb_cursor = test_pipelinedb_conn.cursor()
test_postgres_cursor = test_postgres_conn.cursor()

logging.info('在test_size表中插入数据')
drop_sql = """
select pipelinedb.truncate_continuous_view('test_size_stats');
"""
test_pipelinedb_cursor.execute(drop_sql)
test_pipelinedb_conn.commit()

insert_sql = """
    insert into test_size (size) values (4369)
"""

test_postgres_cursor.execute(insert_sql)
test_postgres_conn.commit()

logging.info('插入数据成功')
time.sleep(3)
logging.info('查询test_size_stats数据')
select_sql = """
    select size from test_size_stats;
"""

test_postgres_cursor.execute(select_sql)
result = test_postgres_cursor.fetchall()
test_postgres_conn.commit()
if result[0][0] == 4369:
    logging.info('数据查询校验成功')

logging.info('测试完毕')











