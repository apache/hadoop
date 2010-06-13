package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AScan extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AScan\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"startRow\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"stopRow\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"columns\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AColumn\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":[\"bytes\",\"null\"]}]}},\"null\"]},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]},{\"name\":\"timerange\",\"type\":[{\"type\":\"record\",\"name\":\"ATimeRange\",\"fields\":[{\"name\":\"minStamp\",\"type\":\"long\"},{\"name\":\"maxStamp\",\"type\":\"long\"}]},\"null\"]},{\"name\":\"maxVersions\",\"type\":[\"int\",\"null\"]}]}");
  public java.nio.ByteBuffer startRow;
  public java.nio.ByteBuffer stopRow;
  public org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AColumn> columns;
  public java.lang.Long timestamp;
  public org.apache.hadoop.hbase.avro.generated.ATimeRange timerange;
  public java.lang.Integer maxVersions;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return startRow;
    case 1: return stopRow;
    case 2: return columns;
    case 3: return timestamp;
    case 4: return timerange;
    case 5: return maxVersions;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: startRow = (java.nio.ByteBuffer)value$; break;
    case 1: stopRow = (java.nio.ByteBuffer)value$; break;
    case 2: columns = (org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AColumn>)value$; break;
    case 3: timestamp = (java.lang.Long)value$; break;
    case 4: timerange = (org.apache.hadoop.hbase.avro.generated.ATimeRange)value$; break;
    case 5: maxVersions = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
