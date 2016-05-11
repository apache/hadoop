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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.StorageStatistics;

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This storage statistics tracks how many times each DFS operation was issued.
 *
 * For each tracked DFS operation, there is a respective entry in the enum
 * {@link OpType}. To use, increment the value the {@link DistributedFileSystem}
 * and {@link org.apache.hadoop.hdfs.web.WebHdfsFileSystem}.
 *
 * This class is thread safe, and is generally shared by multiple threads.
 */
public class DFSOpsCountStatistics extends StorageStatistics {

  /** This is for counting file system operations. */
  public enum OpType {
    ALLOW_SNAPSHOT("allowSnapshot"),
    APPEND("append"),
    CONCAT("concat"),
    COPY_FROM_LOCAL_FILE("copyFromLocalFile"),
    CREATE("create"),
    CREATE_NON_RECURSIVE("createNonRecursive"),
    CREATE_SNAPSHOT("createSnapshot"),
    CREATE_SYM_LINK("createSymlink"),
    DELETE("delete"),
    DELETE_SNAPSHOT("deleteSnapshot"),
    DISALLOW_SNAPSHOT("disallowSnapshot"),
    EXISTS("exists"),
    GET_BYTES_WITH_FUTURE_GS("getBytesWithFutureGenerationStamps"),
    GET_CONTENT_SUMMARY("getContentSummary"),
    GET_FILE_BLOCK_LOCATIONS("getFileBlockLocations"),
    GET_FILE_CHECKSUM("getFileChecksum"),
    GET_FILE_LINK_STATUS("getFileLinkStatus"),
    GET_FILE_STATUS("getFileStatus"),
    GET_LINK_TARGET("getLinkTarget"),
    GET_QUOTA_USAGE("getQuotaUsage"),
    GET_STATUS("getStatus"),
    GET_STORAGE_POLICIES("getStoragePolicies"),
    GET_STORAGE_POLICY("getStoragePolicy"),
    GET_XATTR("getXAttr"),
    LIST_LOCATED_STATUS("listLocatedStatus"),
    LIST_STATUS("listStatus"),
    MKDIRS("mkdirs"),
    MODIFY_ACL_ENTRIES("modifyAclEntries"),
    OPEN("open"),
    PRIMITIVE_CREATE("primitiveCreate"),
    PRIMITIVE_MKDIR("primitiveMkdir"),
    REMOVE_ACL("removeAcl"),
    REMOVE_ACL_ENTRIES("removeAclEntries"),
    REMOVE_DEFAULT_ACL("removeDefaultAcl"),
    REMOVE_XATTR("removeXAttr"),
    RENAME("rename"),
    RENAME_SNAPSHOT("renameSnapshot"),
    RESOLVE_LINK("resolveLink"),
    SET_ACL("setAcl"),
    SET_OWNER("setOwner"),
    SET_PERMISSION("setPermission"),
    SET_REPLICATION("setReplication"),
    SET_STORAGE_POLICY("setStoragePolicy"),
    SET_TIMES("setTimes"),
    SET_XATTR("setXAttr"),
    TRUNCATE("truncate"),
    UNSET_STORAGE_POLICY("unsetStoragePolicy");

    private final String symbol;

    OpType(String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }

    public static OpType fromSymbol(String symbol) {
      if (symbol != null) {
        for (OpType opType : values()) {
          if (opType.getSymbol().equals(symbol)) {
            return opType;
          }
        }
      }
      return null;
    }
  }

  public static final String NAME = "DFSOpsCountStatistics";

  private final Map<OpType, AtomicLong> opsCount = new EnumMap<>(OpType.class);

  public DFSOpsCountStatistics() {
    super(NAME);
    for (OpType opType : OpType.values()) {
      opsCount.put(opType, new AtomicLong(0));
    }
  }

  public void incrementOpCounter(OpType op) {
    opsCount.get(op).addAndGet(1);
  }

  private class LongIterator implements Iterator<LongStatistic> {
    private Iterator<Entry<OpType, AtomicLong>> iterator =
        opsCount.entrySet().iterator();

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public LongStatistic next() {
      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }
      final Entry<OpType, AtomicLong> entry = iterator.next();
      return new LongStatistic(entry.getKey().name(), entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    return new LongIterator();
  }

  @Override
  public Long getLong(String key) {
    final OpType type = OpType.fromSymbol(key);
    return type == null ? null : opsCount.get(type).get();
  }

  @Override
  public boolean isTracked(String key) {
    return OpType.fromSymbol(key) == null;
  }

}
