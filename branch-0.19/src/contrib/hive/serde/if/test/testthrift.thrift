
namespace java org.apache.hadoop.hive.serde.test


struct InnerStruct {
  i32 field0
}

struct ThriftTestObj {
  i32 field1,
  string field2,
  list<InnerStruct> field3
}
