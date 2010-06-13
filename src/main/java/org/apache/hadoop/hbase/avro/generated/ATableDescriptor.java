package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class ATableDescriptor extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ATableDescriptor\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"families\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AFamilyDescriptor\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"compression\",\"type\":[{\"type\":\"enum\",\"name\":\"ACompressionAlgorithm\",\"symbols\":[\"LZO\",\"GZ\",\"NONE\"]},\"null\"]},{\"name\":\"maxVersions\",\"type\":[\"int\",\"null\"]},{\"name\":\"blocksize\",\"type\":[\"int\",\"null\"]},{\"name\":\"inMemory\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"timeToLive\",\"type\":[\"int\",\"null\"]},{\"name\":\"blockCacheEnabled\",\"type\":[\"boolean\",\"null\"]}]}},\"null\"]},{\"name\":\"maxFileSize\",\"type\":[\"long\",\"null\"]},{\"name\":\"memStoreFlushSize\",\"type\":[\"long\",\"null\"]},{\"name\":\"rootRegion\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"metaRegion\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"metaTable\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"readOnly\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"deferredLogFlush\",\"type\":[\"boolean\",\"null\"]}]}");
  public java.nio.ByteBuffer name;
  public org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor> families;
  public java.lang.Long maxFileSize;
  public java.lang.Long memStoreFlushSize;
  public java.lang.Boolean rootRegion;
  public java.lang.Boolean metaRegion;
  public java.lang.Boolean metaTable;
  public java.lang.Boolean readOnly;
  public java.lang.Boolean deferredLogFlush;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return families;
    case 2: return maxFileSize;
    case 3: return memStoreFlushSize;
    case 4: return rootRegion;
    case 5: return metaRegion;
    case 6: return metaTable;
    case 7: return readOnly;
    case 8: return deferredLogFlush;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.nio.ByteBuffer)value$; break;
    case 1: families = (org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor>)value$; break;
    case 2: maxFileSize = (java.lang.Long)value$; break;
    case 3: memStoreFlushSize = (java.lang.Long)value$; break;
    case 4: rootRegion = (java.lang.Boolean)value$; break;
    case 5: metaRegion = (java.lang.Boolean)value$; break;
    case 6: metaTable = (java.lang.Boolean)value$; break;
    case 7: readOnly = (java.lang.Boolean)value$; break;
    case 8: deferredLogFlush = (java.lang.Boolean)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
