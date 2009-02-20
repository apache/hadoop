#!/usr/local/bin/thrift -java
#
# Thrift Service that the MetaStore is built on
#

include "thrift/fb303/if/fb303.thrift"

namespace java org.apache.hadoop.hive.metastore.api
php_namespace metastore

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name, // name of the field
  2: string type, // type of the field. primitive types defined above, specify list<TYPE_NAME>, map<TYPE_NAME, TYPE_NAME> for lists & maps 
  3: string comment
}

struct Type {
  1: string          name,             // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  2: optional string type1,            // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  3: optional string type2,            // val type if the name is 'map' (MAP_TYPE)
  4: optional list<FieldSchema> fields // if the name is one of the user defined types
}

// namespace for tables
struct Database {
  1: string name,
  2: string description,
}

// This object holds the information needed by SerDes
struct SerDeInfo {
  1: string name,                   // name of the serde, table name by default
  2: string serializationLib,       // usually the class that implements the extractor & loader
  3: map<string, string> parameters // initialization parameters
}

// sort order of a column (column name along with asc(1)/desc(0))
struct Order {
  1: string col,  // sort column name
  2: i32    order // asc(1) or desc(0)
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters // any user supplied key value hash
}

// table information
struct Table {
  1: string tableName,                // name of the table
  2: string dbName,                   // database name ('default')
  3: string owner,                    // owner of this table
  4: i32    createTime,               // creation time of the table
  5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  6: i32    retention,                // retention time
  7: StorageDescriptor sd,            // storage descriptor of the table
  8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
  9: map<string, string> parameters   // to store comments or any other user level parameters
}

struct Partition {
  1: list<string> values // string value is converted to appropriate partition key type
  2: string       dbName,
  3: string       tableName,
  4: i32          createTime,
  5: i32          lastAccessTime,
  6: StorageDescriptor   sd,
  7: map<string, string> parameters
}

// index on a hive table is also another table whose columns are the subset of the base table columns along with the offset
// this will automatically generate table (table_name_index_name)
struct Index {
  1: string       indexName, // unique with in the whole database namespace
  2: i32          indexType, // reserved
  3: string       tableName,
  4: string       dbName,
  5: list<string> colNames,  // for now columns will be sorted in the ascending order
}

exception MetaException {
  string message
}

exception UnknownTableException {
  string message
}

exception UnknownDBException {
  string message
}

exception AlreadyExistsException {
  string message
}

exception InvalidObjectException {
  string message
}

exception ExistingDependentsException {
  string message
}

exception NoSuchObjectException {
  string message
}

exception IndexAlreadyExistsException {
  string message
}

exception InvalidOperationException {
  string message
}

/**
* This interface is live.
*/
service ThriftHiveMetastore extends fb303.FacebookService
{
  bool create_database(1:string name, 2:string description) 
                                       throws(1:AlreadyExistsException o1, 2:MetaException o2)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_database(1:string name)    throws(2:MetaException o2)
  list<string> get_databases()         throws(1:MetaException o1)

  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name)  throws(1:MetaException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o2)
  map<string, Type> get_type_all(1:string name) 
                                throws(1:MetaException o2)

  list<FieldSchema> get_fields(string db_name, string table_name) throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // create a Hive table. Following fields must be set
  // tableName
  // database        (only 'default' for now until Hive QL supports databases)
  // owner           (not needed, but good to have for tracking purposes)
  // sd.cols         (list of field schemas)
  // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
  // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData) 
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  list<string> get_tables(string db_name, string pattern) 
                       throws (MetaException o1)

  Table get_table(1:string dbname, 2:string tbl_name) 
                       throws (1:MetaException o1, 2:NoSuchObjectException o2)
  // alter table applies to only future partitions not for existing partitions
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl) 
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // the following applies to only tables that have partitions
  Partition add_partition(1:Partition new_part) 
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals) 
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData) 
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals) 
                       throws(1:MetaException o1)
  // returns all the partitions for this table in reverse chronological order. 
  // if max parts is given then it will return only that many
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1) 
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1) 
                       throws(1:MetaException o2)
  // set new storage descriptor. all partitions should belong to the same table
  bool alter_partitions(1:StorageDescriptor sd, 2:list<string> parts) 
                       throws(1:InvalidOperationException o1, 2:MetaException o2)

  // index related metadata (may not be in the open source)
  bool create_index(1:Index index_def) throws(1:IndexAlreadyExistsException o1, 2:MetaException o2)
}


/**
* This interface is deprecated.
*/
service ThriftMetaStore extends fb303.FacebookService
{
  // retrieve a printable representation of the fields in a table (logfile, type) or table subtype
  list<FieldSchema> get_fields(string db_name, string table_name) throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // get all the tables (logfiles, types) in the metastore - no partitioning like different dbs yet
  list<string> get_tables(string db_name, string pattern)  throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // retrieve the opaque schema representation of this table (logfile, type) which contains enough
  // information for the caller to instantiate some kind of object that will let it examine the type.
  // That object might be a thrift, jute, or SerDe.
  map<string,string> get_schema(string table_name) throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // add some structure to the table or change its structure
  void alter_table(string db_name, string table_name, map<string,string> schema) throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // create_table == create_table4 (table_name, SIMPLE_META_SERDE, '\t', "",  dict [ META_COLUMNS => columns]
  // bugbug do above transformation and deprecate this API
  void create_table(string db_name, string table_name, map<string,string> schema) throws (MetaException o1, UnknownDBException o2),

  // drop a table (i.e., remove it from the metastore) - for now allow metastore to do the delete (so python shell can do drops)
  void drop_table(string db_name, string table_name) throws  (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // truncate a table - i.e., delete its data, but keep the hdfs directory and the schema
  void truncate_table(string db_name, string table_name, string partition)  throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // generally does the table exist
  bool table_exists(string db_name, string table_name) throws (MetaException o1, UnknownDBException o2),

  // create a table with named columns
  list<string> get_partitions(string db_name, string table_name) throws (MetaException o1, UnknownTableException o2, UnknownDBException o3),

  // enumerate all the databases in this store
  list<string> get_dbs() throws  (MetaException o),

  // /bin/cat the table in human readable format
  list<string> cat(string db_name, string table_name,string partition, i32 high) throws  (MetaException o1, UnknownDBException o2, UnknownTableException o3),
}

// these should be needed only for backward compatibility with filestore
const string META_TABLE_COLUMNS   = "columns",
const string BUCKET_FIELD_NAME    = "bucket_field_name",
const string BUCKET_COUNT         = "bucket_count",
const string FIELD_TO_DIMENSION   = "field_to_dimension",
const string META_TABLE_NAME      = "name",
const string META_TABLE_DB        = "db",
const string META_TABLE_LOCATION  = "location",
const string META_TABLE_SERDE     = "serde",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string FILE_INPUT_FORMAT    = "file.inputformat",
const string FILE_OUTPUT_FORMAT   = "file.outputformat",


