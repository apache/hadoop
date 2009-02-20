
namespace java org.apache.hadoop.hive.serde
namespace php org.apache.hadoop.hive.serde
namespace py org_apache_hadoop_hive_serde

  // name of serialization scheme.
const string SERIALIZATION_LIB = "serialization.lib"
const string SERIALIZATION_CLASS = "serialization.class"
const string SERIALIZATION_FORMAT = "serialization.format"
const string SERIALIZATION_DDL = "serialization.ddl"
const string SERIALIZATION_NULL_FORMAT = "serialization.null.format"

const string FIELD_DELIM = "field.delim"
const string COLLECTION_DELIM = "colelction.delim"
const string LINE_DELIM = "line.delim"
const string MAPKEY_DELIM = "mapkey.delim"

typedef string PrimitiveType
typedef string CollectionType

const string TINYINT_TYPE_NAME   = "tinyint";
const string INT_TYPE_NAME       = "int";
const string BIGINT_TYPE_NAME    = "bigint";
const string FLOAT_TYPE_NAME     = "float";
const string DOUBLE_TYPE_NAME    = "double"; 
const string STRING_TYPE_NAME    = "string";
const string DATE_TYPE_NAME      = "date";
const string DATETIME_TYPE_NAME  = "datetime";
const string TIMESTAMP_TYPE_NAME = "timestamp";

const string LIST_TYPE_NAME = "array";
const string MAP_TYPE_NAME  = "map";

const set<string> PrimitiveTypes  = [ TINYINT_TYPE_NAME INT_TYPE_NAME BIGINT_TYPE_NAME FLOAT_TYPE_NAME DOUBLE_TYPE_NAME STRING_TYPE_NAME  DATE_TYPE_NAME DATETIME_TYPE_NAME TIMESTAMP_TYPE_NAME ],
const set<string> CollectionTypes = [ LIST_TYPE_NAME MAP_TYPE_NAME ],


