package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AServerInfo extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AServerInfo\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"infoPort\",\"type\":\"int\"},{\"name\":\"load\",\"type\":{\"type\":\"record\",\"name\":\"AServerLoad\",\"fields\":[{\"name\":\"load\",\"type\":\"int\"},{\"name\":\"maxHeapMB\",\"type\":\"int\"},{\"name\":\"memStoreSizeInMB\",\"type\":\"int\"},{\"name\":\"numberOfRegions\",\"type\":\"int\"},{\"name\":\"numberOfRequests\",\"type\":\"int\"},{\"name\":\"regionsLoad\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ARegionLoad\",\"fields\":[{\"name\":\"memStoreSizeMB\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"storefileIndexSizeMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeMB\",\"type\":\"int\"},{\"name\":\"stores\",\"type\":\"int\"}]}}},{\"name\":\"storefileIndexSizeInMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeInMB\",\"type\":\"int\"},{\"name\":\"usedHeapMB\",\"type\":\"int\"}]}},{\"name\":\"serverAddress\",\"type\":{\"type\":\"record\",\"name\":\"AServerAddress\",\"fields\":[{\"name\":\"hostname\",\"type\":\"string\"},{\"name\":\"inetSocketAddress\",\"type\":\"string\"},{\"name\":\"port\",\"type\":\"int\"}]}},{\"name\":\"serverName\",\"type\":\"string\"},{\"name\":\"startCode\",\"type\":\"long\"}]}");
  public int infoPort;
  public org.apache.hadoop.hbase.avro.generated.AServerLoad load;
  public org.apache.hadoop.hbase.avro.generated.AServerAddress serverAddress;
  public org.apache.avro.util.Utf8 serverName;
  public long startCode;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return infoPort;
    case 1: return load;
    case 2: return serverAddress;
    case 3: return serverName;
    case 4: return startCode;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: infoPort = (java.lang.Integer)value$; break;
    case 1: load = (org.apache.hadoop.hbase.avro.generated.AServerLoad)value$; break;
    case 2: serverAddress = (org.apache.hadoop.hbase.avro.generated.AServerAddress)value$; break;
    case 3: serverName = (org.apache.avro.util.Utf8)value$; break;
    case 4: startCode = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
