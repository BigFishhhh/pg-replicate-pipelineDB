# pg-replicate-pipelineDB
Replication PostreSQL to pipelineDB via Logical replication slots


#### wal2json

*Note:* Use my fork as we tested it to be 100% sure it work with it and no breaking changes happen, [wal2json](https://github.com/hmilkovi/wal2json) .

```
$ git clone https://github.com/hmilkovi/wal2json.git
$ PATH=/path/to/bin/pg_config:$PATH
$ USE_PGXS=1 make
$ USE_PGXS=1 make install
```

#### decoderbufs

*Note:* Use my fork as we tested it to be 100% sure it work with it and no breaking changes happen, [decoderbufs](https://github.com/debezium/postgres-decoderbufs) .

```
$ git clone https://github.com/debezium/postgres-decoderbufs
$ make & make install
```

You need to set up at least two parameters at postgresql.conf:
```
wal_level = logical
max_replication_slots = 1
shared_preload_libraries = 'decoderbufs' # wal2json
```
After changing these parameters, a restart is needed.

### Usage

```
pg_replicate_pipelineDB --config=<absolute path to json（yaml） config >
```

Construct configuration file in json format where:

* **replication_slot** json object for replication slot name and if is temporary
* **tables** is array of tables we want to replicate
* **pipelinedb** connection string to pipelinedb
* **postgres** json object for PostreSQL connection
* **inital_sync** boolean for inital syncronization that needs to be done first time
to replicate old data

Example configuration
```
{
	"plugin":"wal2json",
	"replication_slot": {
		"name": "pipelinedb_slot",
		"is_temp": false
	},
	"tables": [
		{
		"name": "nt_sale_order",
		"primary_key": "id",
		"exclude_columns": "",
		"include_columns": "name,state",
		"filters":[
			{
				"columns": "state",
				"before": "process",
				"after": "done"
			}
		]
	}, {
		"name": "nt_sale_order_line",
		"primary_key": "id",
		"exclude_columns": "",
		"include_columns": "order_id,order_state",
		"filters":[
			{
				"columns": "order_state",
				"before": "process",
				"after": "done"
			}
		]
	},
	{
		"name": "nt_salesman_performance",
		"primary_key": "id",
		"exclude_columns": "",
		"include_columns": "salesman_id"

	},
		{
		"name": "test_size",
		"primary_key": "id",
		"exclude_columns": "",
		"include_columns": "size"

	}
	],
	"pipelinedb": {
		"port": 5432,
		"host": "127.0.0.1",
		"database": "",
		"username": "",
		"password": ""
	},
	"postgres": {
		"port": 5432,
		"host": "127.0.0.1",
		"database": "",
		"username": "",
		"password": ""
	},
	"inital_sync": false,
}
```

### Note

wal2json is not my software so for licence check [their licence](https://raw.githubusercontent.com/hmilkovi/wal2json/master/LICENSE)

decoderbufs is not my software so for licence check [their licence](https://github.com/debezium/postgres-decoderbufs/blob/master/LICENSE)

