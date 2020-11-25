/**
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
package org.apache.hadoop.hdfs.protocol;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.IntrusiveCollection.Element;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Namenode class that tracks state related to a cached path.
 *
 * This is an implementation class, not part of the public API.
 */
@InterfaceAudience.Private
public final class CacheDirective implements IntrusiveCollection.Element {
  private final long id;
  private final String path;
  private final short replication;
  private CachePool pool;
  private final long expiryTime;

  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  private Element prev;
  private Element next;

  public CacheDirective(CacheDirectiveInfo info) {
    this(
        info.getId(),
        info.getPath().toUri().getPath(),
        info.getReplication(),
        info.getExpiration().getAbsoluteMillis());
  }

  public CacheDirective(long id, String path,
      short replication, long expiryTime) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    this.path = checkNotNull(path);
    Preconditions.checkArgument(replication > 0);
    this.replication = replication;
    this.expiryTime = expiryTime;
  }

  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public short getReplication() {
    return replication;
  }

  public CachePool getPool() {
    return pool;
  }

  /**
   * @return When this directive expires, in milliseconds since Unix epoch
   */
  public long getExpiryTime() {
    return expiryTime;
  }

  /**
   * @return When this directive expires, as an ISO-8601 formatted string.
   */
  public String getExpiryTimeString() {
    return DFSUtil.dateToIso8601String(new Date(expiryTime));
  }

  /**
   * Returns a {@link CacheDirectiveInfo} based on this CacheDirective.
   * <p>
   * This always sets an absolute expiry time, never a relative TTL.
   */
  public CacheDirectiveInfo toInfo() {
    return new CacheDirectiveInfo.Builder().
        setId(id).
        setPath(new Path(path)).
        setReplication(replication).
        setPool(pool.getPoolName()).
        setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(expiryTime)).
        build();
  }

  public CacheDirectiveStats toStats() {
    return new CacheDirectiveStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setFilesNeeded(filesNeeded).
        setFilesCached(filesCached).
        setHasExpired(new Date().getTime() > expiryTime).
        build();
  }

  public CacheDirectiveEntry toEntry() {
    return new CacheDirectiveEntry(toInfo(), toStats());
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ id:").append(id).
      append(", path:").append(path).
      append(", replication:").append(replication).
      append(", pool:").append(pool).
      append(", expiryTime: ").append(getExpiryTimeString()).
      append(", bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", filesNeeded:").append(filesNeeded).
      append(", filesCached:").append(filesCached).
      append(" }");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CacheDirective other = (CacheDirective)o;
    return id == other.id;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }

  //
  // Stats related getters and setters
  //

  /**
   * Resets the byte and file statistics being tracked by this CacheDirective.
   */
  public void resetStatistics() {
    bytesNeeded = 0;
    bytesCached = 0;
    filesNeeded = 0;
    filesCached = 0;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public void addBytesNeeded(long bytes) {
    this.bytesNeeded += bytes;
    pool.addBytesNeeded(bytes);
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public void addBytesCached(long bytes) {
    this.bytesCached += bytes;
    pool.addBytesCached(bytes);
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public void addFilesNeeded(long files) {
    this.filesNeeded += files;
    pool.addFilesNeeded(files);
  }

  public long getFilesCached() {
    return filesCached;
  }

  public void addFilesCached(long files) {
    this.filesCached += files;
    pool.addFilesCached(files);
  }

  //
  // IntrusiveCollection.Element implementation
  //

  @SuppressWarnings("unchecked")
  @Override // IntrusiveCollection.Element
  public void insertInternal(IntrusiveCollection<? extends Element> list,
      Element prev, Element next) {
    assert this.pool == null;
    this.pool = ((CachePool.DirectiveList)list).getCachePool();
    this.prev = prev;
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  public void setPrev(IntrusiveCollection<? extends Element> list, Element prev) {
    assert list == pool.getDirectiveList();
    this.prev = prev;
  }

  @Override // IntrusiveCollection.Element
  public void setNext(IntrusiveCollection<? extends Element> list, Element next) {
    assert list == pool.getDirectiveList();
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  public void removeInternal(IntrusiveCollection<? extends Element> list) {
    assert list == pool.getDirectiveList();
    this.pool = null;
    this.prev = null;
    this.next = null;
  }

  @Override // IntrusiveCollection.Element
  public Element getPrev(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.prev;
  }

  @Override // IntrusiveCollection.Element
  public Element getNext(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.next;
  }

  @Override // IntrusiveCollection.Element
  public boolean isInList(IntrusiveCollection<? extends Element> list) {
    return pool == null ? false : list == pool.getDirectiveList();
  }
};
