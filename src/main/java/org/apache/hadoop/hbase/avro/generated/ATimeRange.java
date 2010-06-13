package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class ATimeRange extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ATimeRange\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"minStamp\",\"type\":\"long\"},{\"name\":\"maxStamp\",\"type\":\"long\"}]}");
  public long minStamp;
  public long maxStamp;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return minStamp;
    case 1: return maxStamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: minStamp = (java.lang.Long)value$; break;
    case 1: maxStamp = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
