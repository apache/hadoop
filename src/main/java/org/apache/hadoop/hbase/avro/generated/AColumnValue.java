package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AColumnValue extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AColumnValue\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]}]}");
  public java.nio.ByteBuffer family;
  public java.nio.ByteBuffer qualifier;
  public java.nio.ByteBuffer value;
  public java.lang.Long timestamp;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return family;
    case 1: return qualifier;
    case 2: return value;
    case 3: return timestamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: family = (java.nio.ByteBuffer)value$; break;
    case 1: qualifier = (java.nio.ByteBuffer)value$; break;
    case 2: value = (java.nio.ByteBuffer)value$; break;
    case 3: timestamp = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
