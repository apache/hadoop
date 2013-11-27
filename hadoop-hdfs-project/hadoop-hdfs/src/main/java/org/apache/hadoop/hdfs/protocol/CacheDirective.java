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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.IntrusiveCollection.Element;

import com.google.common.base.Preconditions;

/**
 * Represents an entry in the PathBasedCache on the NameNode.
 *
 * This is an implementation class, not part of the public API.
 */
@InterfaceAudience.Private
public final class CacheDirective implements IntrusiveCollection.Element {
  private final long id;
  private final String path;
  private final short replication;
  private CachePool pool;
  private long bytesNeeded;
  private long bytesCached;
  private long filesAffected;
  private Element prev;
  private Element next;

  public CacheDirective(long id, String path,
      short replication) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    Preconditions.checkArgument(replication > 0);
    this.path = path;
    this.replication = replication;
    Preconditions.checkNotNull(path);
    this.bytesNeeded = 0;
    this.bytesCached = 0;
    this.filesAffected = 0;
  }

  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public CachePool getPool() {
    return pool;
  }

  public short getReplication() {
    return replication;
  }

  public CacheDirectiveInfo toInfo() {
    return new CacheDirectiveInfo.Builder().
        setId(id).
        setPath(new Path(path)).
        setReplication(replication).
        setPool(pool.getPoolName()).
        build();
  }

  public CacheDirectiveStats toStats() {
    return new CacheDirectiveStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setFilesAffected(filesAffected).
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
      append(", bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", filesAffected:").append(filesAffected).
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
    return new HashCodeBuilder().append(id).toHashCode();
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public void clearBytesNeeded() {
    this.bytesNeeded = 0;
  }

  public void addBytesNeeded(long toAdd) {
    this.bytesNeeded += toAdd;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public void clearBytesCached() {
    this.bytesCached = 0;
  }

  public void addBytesCached(long toAdd) {
    this.bytesCached += toAdd;
  }

  public long getFilesAffected() {
    return filesAffected;
  }

  public void clearFilesAffected() {
    this.filesAffected = 0;
  }

  public void incrementFilesAffected() {
    this.filesAffected++;
  }

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
