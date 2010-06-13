package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class APut extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"APut\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"columnValues\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AColumnValue\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]}]}}}]}");
  public java.nio.ByteBuffer row;
  public org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AColumnValue> columnValues;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return row;
    case 1: return columnValues;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: row = (java.nio.ByteBuffer)value$; break;
    case 1: columnValues = (org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AColumnValue>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
