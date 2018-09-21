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
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.util.EnumMap;
import java.util.HashMap;
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

  /** This is for counting distributed file system operations. */
  public enum OpType {
    ALLOW_SNAPSHOT("op_allow_snapshot"),
    APPEND(CommonStatisticNames.OP_APPEND),
    CONCAT("op_concat"),
    COPY_FROM_LOCAL_FILE(CommonStatisticNames.OP_COPY_FROM_LOCAL_FILE),
    CREATE(CommonStatisticNames.OP_CREATE),
    CREATE_NON_RECURSIVE(CommonStatisticNames.OP_CREATE_NON_RECURSIVE),
    CREATE_SNAPSHOT("op_create_snapshot"),
    CREATE_SYM_LINK("op_create_symlink"),
    DELETE(CommonStatisticNames.OP_DELETE),
    DELETE_SNAPSHOT("op_delete_snapshot"),
    DISALLOW_SNAPSHOT("op_disallow_snapshot"),
    EXISTS(CommonStatisticNames.OP_EXISTS),
    GET_BYTES_WITH_FUTURE_GS("op_get_bytes_with_future_generation_stamps"),
    GET_CONTENT_SUMMARY(CommonStatisticNames.OP_GET_CONTENT_SUMMARY),
    GET_FILE_BLOCK_LOCATIONS("op_get_file_block_locations"),
    GET_FILE_CHECKSUM(CommonStatisticNames.OP_GET_FILE_CHECKSUM),
    GET_FILE_LINK_STATUS("op_get_file_link_status"),
    GET_FILE_STATUS(CommonStatisticNames.OP_GET_FILE_STATUS),
    GET_LINK_TARGET("op_get_link_target"),
    GET_QUOTA_USAGE("op_get_quota_usage"),
    GET_STATUS(CommonStatisticNames.OP_GET_STATUS),
    GET_STORAGE_POLICIES("op_get_storage_policies"),
    GET_STORAGE_POLICY("op_get_storage_policy"),
    GET_TRASH_ROOT("op_get_trash_root"),
    GET_XATTR("op_get_xattr"),
    LIST_LOCATED_STATUS(CommonStatisticNames.OP_LIST_LOCATED_STATUS),
    LIST_STATUS(CommonStatisticNames.OP_LIST_STATUS),
    MKDIRS(CommonStatisticNames.OP_MKDIRS),
    MODIFY_ACL_ENTRIES(CommonStatisticNames.OP_MODIFY_ACL_ENTRIES),
    OPEN(CommonStatisticNames.OP_OPEN),
    PRIMITIVE_CREATE("op_primitive_create"),
    PRIMITIVE_MKDIR("op_primitive_mkdir"),
    REMOVE_ACL(CommonStatisticNames.OP_REMOVE_ACL),
    REMOVE_ACL_ENTRIES(CommonStatisticNames.OP_REMOVE_ACL_ENTRIES),
    REMOVE_DEFAULT_ACL(CommonStatisticNames.OP_REMOVE_DEFAULT_ACL),
    REMOVE_XATTR("op_remove_xattr"),
    RENAME(CommonStatisticNames.OP_RENAME),
    RENAME_SNAPSHOT("op_rename_snapshot"),
    RESOLVE_LINK("op_resolve_link"),
    SET_ACL(CommonStatisticNames.OP_SET_ACL),
    SET_OWNER(CommonStatisticNames.OP_SET_OWNER),
    SET_PERMISSION(CommonStatisticNames.OP_SET_PERMISSION),
    SET_REPLICATION("op_set_replication"),
    SET_STORAGE_POLICY("op_set_storagePolicy"),
    SET_TIMES(CommonStatisticNames.OP_SET_TIMES),
    SET_XATTR("op_set_xattr"),
    GET_SNAPSHOT_DIFF("op_get_snapshot_diff"),
    GET_SNAPSHOTTABLE_DIRECTORY_LIST("op_get_snapshottable_directory_list"),
    TRUNCATE(CommonStatisticNames.OP_TRUNCATE),
    UNSET_STORAGE_POLICY("op_unset_storage_policy");

    private static final Map<String, OpType> SYMBOL_MAP =
        new HashMap<>(OpType.values().length);
    static {
      for (OpType opType : values()) {
        SYMBOL_MAP.put(opType.getSymbol(), opType);
      }
    }

    private final String symbol;

    OpType(String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }

    public static OpType fromSymbol(String symbol) {
      return SYMBOL_MAP.get(symbol);
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
      return new LongStatistic(entry.getKey().getSymbol(),
          entry.getValue().get());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String getScheme() {
    return HdfsConstants.HDFS_URI_SCHEME;
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
    return OpType.fromSymbol(key) != null;
  }

  @Override
  public void reset() {
    for (AtomicLong count : opsCount.values()) {
      count.set(0);
    }
  }

}
