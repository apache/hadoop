package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AColumnFamilyDescriptor extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AColumnFamilyDescriptor\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"compression\",\"type\":{\"type\":\"enum\",\"name\":\"ACompressionAlgorithm\",\"symbols\":[\"LZO\",\"GZ\",\"NONE\"]}},{\"name\":\"maxVersions\",\"type\":\"int\"},{\"name\":\"blocksize\",\"type\":\"int\"},{\"name\":\"inMemory\",\"type\":\"boolean\"},{\"name\":\"timeToLive\",\"type\":\"int\"},{\"name\":\"blockCacheEnabled\",\"type\":\"boolean\"},{\"name\":\"bloomfilterEnabled\",\"type\":\"boolean\"}]}");
  public java.nio.ByteBuffer name;
  public org.apache.hadoop.hbase.avro.generated.ACompressionAlgorithm compression;
  public int maxVersions;
  public int blocksize;
  public boolean inMemory;
  public int timeToLive;
  public boolean blockCacheEnabled;
  public boolean bloomfilterEnabled;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return compression;
    case 2: return maxVersions;
    case 3: return blocksize;
    case 4: return inMemory;
    case 5: return timeToLive;
    case 6: return blockCacheEnabled;
    case 7: return bloomfilterEnabled;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.nio.ByteBuffer)value$; break;
    case 1: compression = (org.apache.hadoop.hbase.avro.generated.ACompressionAlgorithm)value$; break;
    case 2: maxVersions = (java.lang.Integer)value$; break;
    case 3: blocksize = (java.lang.Integer)value$; break;
    case 4: inMemory = (java.lang.Boolean)value$; break;
    case 5: timeToLive = (java.lang.Integer)value$; break;
    case 6: blockCacheEnabled = (java.lang.Boolean)value$; break;
    case 7: bloomfilterEnabled = (java.lang.Boolean)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
