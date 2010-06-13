package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AServerLoad extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AServerLoad\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"load\",\"type\":\"int\"},{\"name\":\"maxHeapMB\",\"type\":\"int\"},{\"name\":\"memStoreSizeInMB\",\"type\":\"int\"},{\"name\":\"numberOfRegions\",\"type\":\"int\"},{\"name\":\"numberOfRequests\",\"type\":\"int\"},{\"name\":\"regionsLoad\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ARegionLoad\",\"fields\":[{\"name\":\"memStoreSizeMB\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"storefileIndexSizeMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeMB\",\"type\":\"int\"},{\"name\":\"stores\",\"type\":\"int\"}]}}},{\"name\":\"storefileIndexSizeInMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeInMB\",\"type\":\"int\"},{\"name\":\"usedHeapMB\",\"type\":\"int\"}]}");
  public int load;
  public int maxHeapMB;
  public int memStoreSizeInMB;
  public int numberOfRegions;
  public int numberOfRequests;
  public org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.ARegionLoad> regionsLoad;
  public int storefileIndexSizeInMB;
  public int storefiles;
  public int storefileSizeInMB;
  public int usedHeapMB;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return load;
    case 1: return maxHeapMB;
    case 2: return memStoreSizeInMB;
    case 3: return numberOfRegions;
    case 4: return numberOfRequests;
    case 5: return regionsLoad;
    case 6: return storefileIndexSizeInMB;
    case 7: return storefiles;
    case 8: return storefileSizeInMB;
    case 9: return usedHeapMB;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: load = (java.lang.Integer)value$; break;
    case 1: maxHeapMB = (java.lang.Integer)value$; break;
    case 2: memStoreSizeInMB = (java.lang.Integer)value$; break;
    case 3: numberOfRegions = (java.lang.Integer)value$; break;
    case 4: numberOfRequests = (java.lang.Integer)value$; break;
    case 5: regionsLoad = (org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.ARegionLoad>)value$; break;
    case 6: storefileIndexSizeInMB = (java.lang.Integer)value$; break;
    case 7: storefiles = (java.lang.Integer)value$; break;
    case 8: storefileSizeInMB = (java.lang.Integer)value$; break;
    case 9: usedHeapMB = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
