#!/usr/local/bin/thrift -java
#
# Thrift Service that the MetaStore is built on
# Pete Wyckoff (pwyckoff@facebook.com)
#

/**
 * The available types in Thrift:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 */

include "thrift/fb303/if/fb303.thrift"


namespace java org.apache.hadoop.hive.metastore.api
namespace php metastore

// below is terrible - tight, tight integration like you know who... bugbug
// need to add a level of indirection to name these things..
const string META_SERDE = "com.facebook.serde.simple_meta.MetadataTypedColumnsetSerDe"

const string META_TABLE_COLUMNS  = "columns",
const string BUCKET_FIELD_NAME   = "bucket_field_name",
const string BUCKET_COUNT        = "bucket_count",
const string FIELD_TO_DIMENSION  = "field_to_dimension",
const string META_TABLE_NAME     = "name",
const string META_TABLE_DB     = "db",
const string META_TABLE_LOCATION = "location",
const string META_TABLE_SERDE    = "serde",
const string SERIALIZATION_FORMAT = "serialization.format",
const string SERIALIZATION_CLASS = "serialization.class",
const string SERIALIZATION_LIB = "serialization.lib",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string FILE_INPUT_FORMAT = "file.inputformat",
const string FILE_OUTPUT_FORMAT = "file.outputformat",


exception MetaException {
  string message
}

exception UnknownTableException {
  string message
}

exception UnknownDBException {
  string message
}

// new metastore api, below will be merged with above

const string KEY_COMMENTS = "key_comments";
const string VERSION_0_1 = "0.1";

typedef string PrimitiveType
typedef string CollectionType

const string TINYINT_TYPE_NAME = "tinyint";
const string INT_TYPE_NAME = "int";
const string BIGINT_TYPE_NAME = "bigint";
const string FLOAT_TYPE_NAME = "float";
const string DOUBLE_TYPE_NAME = "double"; 
const string STRING_TYPE_NAME = "string";
const string DATE_TYPE_NAME = "date";
const string DATETIME_TYPE_NAME = "datetime";
const string TIMESTAMP_TYPE_NAME = "timestamp";

const string LIST_TYPE_NAME = "list";
const string MAP_TYPE_NAME = "map";

const set<string> PrimitiveTypes = [ TINYINT_TYPE_NAME INT_TYPE_NAME BIGINT_TYPE_NAME FLOAT_TYPE_NAME DOUBLE_TYPE_NAME STRING_TYPE_NAME  DATE_TYPE_NAME DATETIME_TYPE_NAME TIMESTAMP_TYPE_NAME ],
const set<string> CollectionTypes = [ LIST_TYPE_NAME MAP_TYPE_NAME ],

struct Version {
  string version,
  string comments
}

struct FieldSchema {
  string name,
  string type,
  string comment
}

struct Type {
  string name, // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  optional string type1, // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  optional string type2, // val type if the name is 'map' (MAP_TYPE)
  optional list<FieldSchema> fields //if the name is one of the user defined types
}

// groups a set of tables
struct Database {
  string name,
  string locationUri,
}

struct SerDeInfo {
  string name;
  string serializationFormat;
  string serializationClass;
  string serializationLib;
  string fieldDelim;
  string collectionItemDelim;
  string mapKeyDelim;
  string lineDelim;
  map<string, string> parameters
}

struct Order {
  string col,
  i32 order
}

struct StorageDescriptor {
  list<FieldSchema> cols,
  string location,
  string inputFormat;
  string outputFormat;
  bool isCompressed;
  i32 numBuckets = 32, // this must be specified if there are any dimension columns
  SerDeInfo serdeInfo;
  list<string> bucketCols, //reducer grouping columns and clustering columns and bucketing columns`
  list<Order> sortCols,
  map<string, string> parameters
}

struct Table {
  string tableName,
  string database,
  string owner,
  i32 createTime,
  i32 lastAccessTime,
  i32 retention,
  StorageDescriptor sd,
  list<FieldSchema> partitionKeys, // optional
  map<string, string> parameters // to store comments or any other user level parameters
}

struct Partition {
  // keys are inherited from table. this should be okay because partition keys can't be changed over time
  list<string> values // string value is converted to appropriate partition key type
  string database,
  string tableName,
  i32 createTime,
  i32 lastAccessTime,
  StorageDescriptor sd,
  map<string, string> parameters
}

// index on a hive table is also another table whose columns are the subset of the base table columns along with the offset
// this will automatically generate table (table_name_index_name)
struct Index {
  string indexName, // unique with in the whole database namespace
  i32 indexType, // reserved
  string tableName,
  string databaseName,
  list<string> colNames, // for now columns will be sorted in the ascending order
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
* This interface is NOT live yet.
*/
service ThriftHiveMetastore extends fb303.FacebookService
{
  // Database
  bool create_database(1:string name, 2:string location_uri) throws(1:AlreadyExistsException o1, 2:MetaException o2)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_database(1:string name) throws(2:MetaException o2)
  list<string> get_databases() throws(1:MetaException o1)

  // Type
  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name) throws(1:MetaException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o2)
  map<string, Type> get_type_all(1:string name) throws(1:MetaException o2)

  list<FieldSchema> get_fields(string db_name, string table_name) throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // Tables
  // create the table with the given table object in the given database
  void create_table(1:Table tbl) throws(1:AlreadyExistsException ouch1, 2:InvalidObjectException ouch2, 3:MetaException ouch3, 4:NoSuchObjectException o4)
  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData) throws(1:NoSuchObjectException o1, 2:MetaException ouch3)
  list<string> get_tables(string db_name, string pattern)  throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3)
  Table get_table(1:string dbname, 2:string tbl_name) throws (1:MetaException o1, 2:NoSuchObjectException o2)
  bool set_table_parameters(1:string dbname, 2:string tbl_name, 3:map<string, string> params) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  // this applies to only future partitions not for existing partitions
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl) throws (1:InvalidOperationException o1, 2:MetaException o2)
  void truncate_table(1:string db_name, 2:string table_name, 3:string partition)  throws (1:MetaException ouch1, 2:UnknownTableException ouch2, 3:UnknownDBException ouch3),
  list<string> cat(1:string db_name, 2:string table_name, 3:string partition, i32 high) throws  (MetaException ouch1, UnknownDBException ouch2, UnknownTableException ouch3),

  // Partition
  // the following applies to only tables that have partitions
  Partition add_partition(1:Partition new_part) throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals) throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals) throws(1:MetaException o1)
  // returns all the partitions for this table in reverse chronological order. if max parts is given then it will return only that many
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  // this can be used if an old parition has to be recreated
  bool set_partition_parameters(1:string db_name, 2:string tbl_name, 3:string pname, 4:map<string, string> params) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  // changes the given partitions to the new storage descriptor. all partitions should belong to the same table
  bool alter_partitions(1:StorageDescriptor sd, 2:list<string> parts) throws(1:InvalidOperationException o1, 2:MetaException o2)

  // Index
  // index related metadata (may not be in the open source)
  bool create_index(1:Index index_def) throws(1:IndexAlreadyExistsException o1, 2:MetaException o2)
}


/**
* This interface is the live.
*/
service ThriftMetaStore extends fb303.FacebookService
{
  // retrieve a printable representation of the fields in a table (logfile, type) or table subtype
  list<FieldSchema> get_fields(string db_name, string table_name) throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // get all the tables (logfiles, types) in the metastore - no partitioning like different dbs yet
  list<string> get_tables(string db_name, string pattern)  throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // retrieve the opaque schema representation of this table (logfile, type) which contains enough
  // information for the caller to instantiate some kind of object that will let it examine the type.
  // That object might be a thrift, jute, or SerDe.
  map<string,string> get_schema(string table_name) throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // add some structure to the table or change its structure
  void alter_table(string db_name, string table_name, map<string,string> schema) throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // create_table == create_table4 (table_name, SIMPLE_META_SERDE, '\t', "",  dict [ META_COLUMNS => columns]
  // bugbug do above transformation and deprecate this API
  void create_table(string db_name, string table_name, map<string,string> schema) throws (MetaException ouch1, UnknownDBException ouch2),

  // drop a table (i.e., remove it from the metastore) - for now allow metastore to do the delete (so python shell can do drops)
  void drop_table(string db_name, string table_name) throws  (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // truncate a table - i.e., delete its data, but keep the hdfs directory and the schema
  void truncate_table(string db_name, string table_name, string partition)  throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // generally does the table exist
  bool table_exists(string db_name, string table_name) throws (MetaException ouch1, UnknownDBException ouch2),

  // create a table with named columns
  list<string> get_partitions(string db_name, string table_name) throws (MetaException ouch1, UnknownTableException ouch2, UnknownDBException ouch3),

  // enumerate all the databases in this store
  list<string> get_dbs() throws  (MetaException ouch),

  // /bin/cat the table in human readable format
  list<string> cat(string db_name, string table_name,string partition, i32 high) throws  (MetaException ouch1, UnknownDBException ouch2, UnknownTableException ouch3),
}


