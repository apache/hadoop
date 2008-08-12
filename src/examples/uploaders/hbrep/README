hbrep is a tool for replicating data from postgresql tables to hbase tables.

Dependancies:
 - python 2.4
 - hbase 0.2.0
 - skytools 2.1.7
 - postgresql
 
It has two main functions.
 - bootstrap, which bootstraps all the data from specified columns of a table
 - play, which processes incoming insert, update and delete events and applies them to hbase.

Example usage:
install triggers:
  ./hbrep.py hbrep.ini install schema1.table1 schema2.table2
now that future updates are queuing, bootstrap the tables.
  ./hbrep.py hbrep.ini bootstrap schema1.table1 schema2.table2
start pgq ticker
  pgqadm.py pgq.ini ticker
play our queue consumer
  ./hbrep.py hbrep.ini play schema1.table1 schema2.table2


More details follow.


All functions require an ini file (say hbrep.ini) with a HBaseReplic section, and a section for each postgresql table you wish to replicate containing the table mapping. Note the table mapping section names should match the name of the postgresql table.

eg. ini file:
####################
[HBaseReplic]
job_name = hbase_replic_job
logfile = %(job_name)s.log
pidfile = %(job_name)s.pid
postgresql_db = dbname=source_database user=dbuser
pgq_queue_name = hbase_replic_queue
hbase_hostname = localhost
hbase_port = 9090
# If omitted, default is 10000
max_batch_size = 10000
# file to use when copying a table, if omitted a select columns will be done instead.
bootstrap_tmpfile = tabledump.dat

# For each table mapping, there must be the same number psql_columns as hbase_column_descriptors
[public.users]
psql_schema = public
psql_table_name = users
psql_key_column = user_id
psql_columns = dob
hbase_table_name = stuff
hbase_column_descriptors = users:dob
hbase_row_prefix = user_id:
####################

Bootstrapping:
To bootstrap the public.users table from postgresql to hbase, 

  ./hbrep.py hbrep.ini bootstrap public.users
  
you can specify multiple tables as arguments.
 
 
Play:
This mode uses pgq from the skytools package to create and manage event queues on postgresql.
You need to have pgq installed on the database you are replicating.

With a pgq.ini file like this:
####################
[pgqadm]
job_name = sourcedb_ticker
db = dbname=source_database user=dbuser
# how often to run maintenance [minutes]
maint_delay_min = 1
# how often to check for activity [secs]
loop_delay = 0.2
logfile = %(job_name)s.log
pidfile = %(job_name)s.pid
use_skylog = 0
####################

You install pgq on the database by, 

  pgqadm.py pgq.ini install

Next you install hbrep.

  hbrep.py hbrep.ini install public.users
  
This creates a queue using pgq, which in this case will be called hbase_replic_queue. It also registers the hbrep consumer (called HBaseReplic) with that queue. Then finally it creates triggers on each table specified to add an event for each insert, update or delete.

Start the pgq event ticker,

  pgqadm.py pgq.ini ticker

Finally, run the hbreplic consumer
  ./hbrep.py hbrep.ini play public.users
  
Now any inserts, updates or deletes on the postgresql users table will be processed and sent to the 
hbase table.


uninstall:
You can remove the triggers from a table by
  ./hbrep.py hbrep.ini uninstall public.users
  
  

