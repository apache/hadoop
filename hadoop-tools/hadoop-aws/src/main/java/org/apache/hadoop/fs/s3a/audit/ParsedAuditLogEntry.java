/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import static org.apache.hadoop.io.WritableUtils.writeString;

/**
 * Log entry which can be serialized as java Serializable or hadoop Writable.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ParsedAuditLogEntry implements Serializable, Writable {

  private static final long serialVersionUID = 0xf00f_f00fL;

  private static final int VERSION = 0xf00f;

  private String owner;

  private String bucket;

  private String timestamp;

  private String remoteip;

  private String requester;

  private String requestid;

  private String verb;

  private String key;

  private String requesturi;

  private String http;

  private String awserrorcode;

  private long bytessent;

  private long objectsize;

  private long totaltime;

  private long turnaroundtime;

  private String referrer;

  private String useragent;

  private String version;

  private String hostid;

  private String sigv;

  private String cypher;

  private String auth;

  private String endpoint;

  private String tls;

  private String tail;

  private Map<String, String> referrerMap;

  public ParsedAuditLogEntry() {
  }

  /**
   * Build from an avro record.
   * @param record record
   */
  public ParsedAuditLogEntry(AvroS3LogEntryRecord record) {
    fromAvro(record);
  }


  public String getOwner() {
    return owner;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public String getAuth() {
    return auth;
  }

  public void setAuth(final String auth) {
    this.auth = auth;
  }

  public String getAwserrorcode() {
    return awserrorcode;
  }

  public void setAwserrorcode(final String awserrorcode) {
    this.awserrorcode = awserrorcode;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(final String bucket) {
    this.bucket = bucket;
  }

  public long getBytessent() {
    return bytessent;
  }

  public void setBytessent(final long bytessent) {
    this.bytessent = bytessent;
  }

  public String getCypher() {
    return cypher;
  }

  public void setCypher(final String cypher) {
    this.cypher = cypher;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(final String endpoint) {
    this.endpoint = endpoint;
  }

  public String getHostid() {
    return hostid;
  }

  public void setHostid(final String hostid) {
    this.hostid = hostid;
  }

  public String getHttp() {
    return http;
  }

  public void setHttp(final String http) {
    this.http = http;
  }

  public String getKey() {
    return key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  public long getObjectsize() {
    return objectsize;
  }

  public void setObjectsize(final long objectsize) {
    this.objectsize = objectsize;
  }

  public String getReferrer() {
    return referrer;
  }

  public void setReferrer(final String referrer) {
    this.referrer = referrer;
  }

  public Map<String, String> getReferrerMap() {
    return referrerMap;
  }

  public void setReferrerMap(final Map<String, String> referrerMap) {
    this.referrerMap = referrerMap;
  }

  /**
   * Get an audit entry, returning the default value if the referrer map is
   * null or there is no entry of that name.
   * @param name entry name
   * @param defVal default value
   * @return the value or the default
   */
  public String getAuditEntry(String name, String defVal) {
    if (referrerMap == null) {
      return null;
    }
    return referrerMap.getOrDefault(name, defVal);
  }

  /**
   * Get an audit entry, returning the default value if the referrer map is
   * null or there is no entry of that name.
   * @param name entry name
   * @param defVal default value
   * @return the value or the default
   */
  public boolean hasAuditEntry(String name) {
    return referrerMap != null &&
        referrerMap.containsKey(name);
  }

  public String getRemoteip() {
    return remoteip;
  }

  public void setRemoteip(final String remoteip) {
    this.remoteip = remoteip;
  }

  public String getRequester() {
    return requester;
  }

  public void setRequester(final String requester) {
    this.requester = requester;
  }

  public String getRequestid() {
    return requestid;
  }

  public void setRequestid(final String requestid) {
    this.requestid = requestid;
  }

  public String getRequesturi() {
    return requesturi;
  }

  public void setRequesturi(final String requesturi) {
    this.requesturi = requesturi;
  }

  public String getSigv() {
    return sigv;
  }

  public void setSigv(final String sigv) {
    this.sigv = sigv;
  }

  public String getTail() {
    return tail;
  }

  public void setTail(final String tail) {
    this.tail = tail;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final String timestamp) {
    this.timestamp = timestamp;
  }

  public String getTls() {
    return tls;
  }

  public void setTls(final String tls) {
    this.tls = tls;
  }

  public long getTotaltime() {
    return totaltime;
  }

  public void setTotaltime(final long totaltime) {
    this.totaltime = totaltime;
  }

  public long getTurnaroundtime() {
    return turnaroundtime;
  }

  public void setTurnaroundtime(final long turnaroundtime) {
    this.turnaroundtime = turnaroundtime;
  }

  public String getUseragent() {
    return useragent;
  }

  public void setUseragent(final String useragent) {
    this.useragent = useragent;
  }

  public String getVerb() {
    return verb;
  }

  public void setVerb(final String verb) {
    this.verb = verb;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  /**
   * Build from an avro record.
   * <p>
   * All {@code CharSequence} fields are converted to strings,
   * and the referrer map is rebuilt.
   * If any of the {@code Long} fields are null, they are set to 0.
   * @param record source record
   */
  public void fromAvro(AvroS3LogEntryRecord record) {

    owner = String.valueOf(record.getOwner());
    bucket = String.valueOf(record.getBucket());
    timestamp = String.valueOf(record.getTimestamp());
    remoteip = String.valueOf(record.getRemoteip());
    requester = String.valueOf(record.getRequester());
    requestid = String.valueOf(record.getRequestid());
    verb = String.valueOf(record.getVerb());
    key = String.valueOf(record.getKey());
    requesturi = String.valueOf(record.getRequesturi());
    http = String.valueOf(record.getHttp());
    awserrorcode = String.valueOf(record.getAwserrorcode());
    bytessent = longValue(record.getBytessent());
    objectsize = longValue(record.getObjectsize());
    totaltime = longValue(record.getTotaltime());
    turnaroundtime = longValue(record.getTurnaroundtime());
    referrer = String.valueOf(record.getReferrer());
    useragent = String.valueOf(record.getUseragent());
    version = String.valueOf(record.getVersion());
    hostid = String.valueOf(record.getHostid());
    sigv = String.valueOf(record.getSigv());
    cypher = String.valueOf(record.getCypher());
    auth = String.valueOf(record.getAuth());
    endpoint = String.valueOf(record.getEndpoint());
    tls = String.valueOf(record.getTls());
    tail = String.valueOf(record.getTail());
    // copy the entries
    final Map<CharSequence, CharSequence> entries = record.getReferrerMap();
    referrerMap = new HashMap<>(entries.size());
    entries.forEach((k, v) ->
        referrerMap.put(String.valueOf(k), String.valueOf(v)));
  }

  /**
   * get a long value if the source is not null.
   * @param l Long source
   * @return either the long value or 0
   */
  private long longValue(Long l) {
    return l != null ? l : 0;
  }

  /**
   * Fill in an avro record.
   * @param record record to update
   * @return the record
   */
  public AvroS3LogEntryRecord toAvro(AvroS3LogEntryRecord record) {
    record.setOwner(owner);
    record.setBucket(bucket);
    record.setTimestamp(timestamp);
    record.setRemoteip(remoteip);
    record.setRequester(requester);
    record.setRequestid(requestid);
    record.setVerb(verb);
    record.setKey(key);
    record.setRequesturi(requesturi);
    record.setHttp(http);
    record.setAwserrorcode(awserrorcode);
    record.setBytessent(bytessent);
    record.setObjectsize(objectsize);
    record.setTotaltime(totaltime);
    record.setTurnaroundtime(turnaroundtime);
    record.setReferrer(referrer);
    record.setUseragent(useragent);
    record.setVersion(version);
    record.setHostid(hostid);
    record.setSigv(sigv);
    record.setCypher(cypher);
    record.setAuth(auth);
    record.setEndpoint(endpoint);
    record.setTls(tls);
    record.setTail(tail);
    Map<CharSequence, CharSequence> entries = new HashMap<>(referrerMap.size());
    entries.putAll(referrerMap);
    record.setReferrerMap(entries);
    return record;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long ver = in.readLong();
    if (ver != serialVersionUID) {
      throw new IOException("Unknown version of ParsedAuditLogEntry: " +
          ver);
    }
    owner = readStr(in);
    bucket = readStr(in);
    timestamp = readStr(in);
    remoteip = readStr(in);
    requester = readStr(in);
    requestid = readStr(in);
    verb = readStr(in);
    key = readStr(in);
    requesturi = readStr(in);
    http = readStr(in);
    awserrorcode = readStr(in);
    bytessent = in.readLong();
    objectsize = in.readLong();
    totaltime = in.readLong();
    turnaroundtime = in.readLong();
    referrer = readStr(in);
    useragent = readStr(in);
    version = readStr(in);
    hostid = readStr(in);
    sigv = readStr(in);
    cypher = readStr(in);
    auth = readStr(in);
    endpoint = readStr(in);
    tls = readStr(in);
    tail = readStr(in);
    // read the referrer map
    final int size = in.readInt();
    referrerMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      referrerMap.put(readStr(in), readStr(in));
    }
  }


  @Override
  public void write(final DataOutput out) throws IOException {

    out.writeLong(serialVersionUID);
    final String s = owner;
    write(out, s);
    write(out, bucket);
    write(out, timestamp);
    write(out, remoteip);
    write(out, requester);
    write(out, requestid);
    write(out, verb);
    write(out, key);
    write(out, requesturi);
    write(out, http);
    write(out, awserrorcode);
    out.writeLong(bytessent);
    out.writeLong(objectsize);
    out.writeLong(totaltime);
    out.writeLong(turnaroundtime);
    write(out, referrer);
    write(out, useragent);
    write(out, version);
    write(out, hostid);
    write(out, sigv);
    write(out, cypher);
    write(out, auth);
    write(out, endpoint);
    write(out, tls);
    write(out, tail);

    // write the referrer map
    out.writeInt(referrerMap.size());
    for (Map.Entry<String, String> entry : referrerMap.entrySet()) {
      write(out, entry.getKey());
      write(out, entry.getValue());
    }

  }

  private static void write(final DataOutput out, final String s) throws IOException {
    writeString(out, s);
  }

  private String readStr(final DataInput in) throws IOException {
    return WritableUtils.readString(in);
  }

  /**
   * Deep equality test on all values.
   * @param that the object to test against
   * @return true iff everything matches
   */
  public boolean deepEquals(final ParsedAuditLogEntry that) {
    if (this == that) {
      return true;
    }
    return bytessent == that.bytessent
        && objectsize == that.objectsize
        && totaltime == that.totaltime
        && turnaroundtime == that.turnaroundtime
        && Objects.equals(owner, that.owner)
        && Objects.equals(bucket, that.bucket)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(remoteip, that.remoteip)
        && Objects.equals(requester, that.requester)
        && Objects.equals(requestid, that.requestid)
        && Objects.equals(verb, that.verb)
        && Objects.equals(key, that.key)
        && Objects.equals(requesturi, that.requesturi)
        && Objects.equals(http, that.http)
        && Objects.equals(awserrorcode, that.awserrorcode)
        && Objects.equals(referrer, that.referrer)
        && Objects.equals(useragent, that.useragent)
        && Objects.equals(version, that.version)
        && Objects.equals(hostid, that.hostid)
        && Objects.equals(sigv, that.sigv)
        && Objects.equals(cypher, that.cypher)
        && Objects.equals(auth, that.auth)
        && Objects.equals(endpoint, that.endpoint)
        && Objects.equals(tls, that.tls)
        && Objects.equals(tail, that.tail)
        && Objects.equals(referrerMap, that.referrerMap); // relies on hashmap equality
  }

  /**
   * Equality is based on the request ID only.
   * @param o other object
   * @return true iff the request IDs match
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {return true;}
    if (!(o instanceof ParsedAuditLogEntry)) {return false;}
    ParsedAuditLogEntry that = (ParsedAuditLogEntry) o;
    return Objects.equals(requestid, that.requestid);
  }

  /**
   * Hashcode is based on the request ID only.
   * @return hash code
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(requestid);
  }

  @Override
  public String toString() {
    return "ParsedAuditLogEntry{" +
        "auth='" + auth + '\'' +
        ", awserrorcode='" + awserrorcode + '\'' +
        ", bucket='" + bucket + '\'' +
        ", bytessent=" + bytessent +
        ", endpoint='" + endpoint + '\'' +
        ", hostid='" + hostid + '\'' +
        ", http='" + http + '\'' +
        ", key='" + key + '\'' +
        ", objectsize=" + objectsize +
        ", owner='" + owner + '\'' +
        ", referrer='" + referrer + '\'' +
        ", referrerMap=" + referrerMap +
        ", remoteip='" + remoteip + '\'' +
        ", requester='" + requester + '\'' +
        ", requestid='" + requestid + '\'' +
        ", requesturi='" + requesturi + '\'' +
        ", tail='" + tail + '\'' +
        ", timestamp='" + timestamp + '\'' +
        ", totaltime=" + totaltime +
        ", turnaroundtime=" + turnaroundtime +
        ", useragent='" + useragent + '\'' +
        ", verb='" + verb + '\'' +
        '}';
  }
}
