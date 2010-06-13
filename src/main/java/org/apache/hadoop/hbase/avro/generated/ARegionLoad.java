package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class ARegionLoad extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ARegionLoad\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"memStoreSizeMB\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"storefileIndexSizeMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeMB\",\"type\":\"int\"},{\"name\":\"stores\",\"type\":\"int\"}]}");
  public int memStoreSizeMB;
  public java.nio.ByteBuffer name;
  public int storefileIndexSizeMB;
  public int storefiles;
  public int storefileSizeMB;
  public int stores;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return memStoreSizeMB;
    case 1: return name;
    case 2: return storefileIndexSizeMB;
    case 3: return storefiles;
    case 4: return storefileSizeMB;
    case 5: return stores;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: memStoreSizeMB = (java.lang.Integer)value$; break;
    case 1: name = (java.nio.ByteBuffer)value$; break;
    case 2: storefileIndexSizeMB = (java.lang.Integer)value$; break;
    case 3: storefiles = (java.lang.Integer)value$; break;
    case 4: storefileSizeMB = (java.lang.Integer)value$; break;
    case 5: stores = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
