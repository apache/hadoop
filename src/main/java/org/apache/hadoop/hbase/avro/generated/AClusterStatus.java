package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AClusterStatus extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AClusterStatus\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"averageLoad\",\"type\":\"double\"},{\"name\":\"deadServerNames\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"deadServers\",\"type\":\"int\"},{\"name\":\"hbaseVersion\",\"type\":\"string\"},{\"name\":\"regionsCount\",\"type\":\"int\"},{\"name\":\"requestsCount\",\"type\":\"int\"},{\"name\":\"serverInfos\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AServerInfo\",\"fields\":[{\"name\":\"infoPort\",\"type\":\"int\"},{\"name\":\"load\",\"type\":{\"type\":\"record\",\"name\":\"AServerLoad\",\"fields\":[{\"name\":\"load\",\"type\":\"int\"},{\"name\":\"maxHeapMB\",\"type\":\"int\"},{\"name\":\"memStoreSizeInMB\",\"type\":\"int\"},{\"name\":\"numberOfRegions\",\"type\":\"int\"},{\"name\":\"numberOfRequests\",\"type\":\"int\"},{\"name\":\"regionsLoad\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ARegionLoad\",\"fields\":[{\"name\":\"memStoreSizeMB\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"storefileIndexSizeMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeMB\",\"type\":\"int\"},{\"name\":\"stores\",\"type\":\"int\"}]}}},{\"name\":\"storefileIndexSizeInMB\",\"type\":\"int\"},{\"name\":\"storefiles\",\"type\":\"int\"},{\"name\":\"storefileSizeInMB\",\"type\":\"int\"},{\"name\":\"usedHeapMB\",\"type\":\"int\"}]}},{\"name\":\"serverAddress\",\"type\":{\"type\":\"record\",\"name\":\"AServerAddress\",\"fields\":[{\"name\":\"hostname\",\"type\":\"string\"},{\"name\":\"inetSocketAddress\",\"type\":\"string\"},{\"name\":\"port\",\"type\":\"int\"}]}},{\"name\":\"serverName\",\"type\":\"string\"},{\"name\":\"startCode\",\"type\":\"long\"}]}}},{\"name\":\"servers\",\"type\":\"int\"}]}");
  public double averageLoad;
  public org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8> deadServerNames;
  public int deadServers;
  public org.apache.avro.util.Utf8 hbaseVersion;
  public int regionsCount;
  public int requestsCount;
  public org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AServerInfo> serverInfos;
  public int servers;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return averageLoad;
    case 1: return deadServerNames;
    case 2: return deadServers;
    case 3: return hbaseVersion;
    case 4: return regionsCount;
    case 5: return requestsCount;
    case 6: return serverInfos;
    case 7: return servers;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: averageLoad = (java.lang.Double)value$; break;
    case 1: deadServerNames = (org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8>)value$; break;
    case 2: deadServers = (java.lang.Integer)value$; break;
    case 3: hbaseVersion = (org.apache.avro.util.Utf8)value$; break;
    case 4: regionsCount = (java.lang.Integer)value$; break;
    case 5: requestsCount = (java.lang.Integer)value$; break;
    case 6: serverInfos = (org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AServerInfo>)value$; break;
    case 7: servers = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
