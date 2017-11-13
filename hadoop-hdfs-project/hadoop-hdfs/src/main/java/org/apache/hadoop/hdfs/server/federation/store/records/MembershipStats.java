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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * Data schema for storing NN stats in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.StateStoreService
 * StateStoreService}.
 */
public abstract class MembershipStats extends BaseRecord {

  public static MembershipStats newInstance() throws IOException {
    MembershipStats record =
        StateStoreSerializer.newRecord(MembershipStats.class);
    record.init();
    return record;
  }

  public abstract void setTotalSpace(long space);

  public abstract long getTotalSpace();

  public abstract void setAvailableSpace(long space);

  public abstract long getAvailableSpace();

  public abstract void setNumOfFiles(long files);

  public abstract long getNumOfFiles();

  public abstract void setNumOfBlocks(long blocks);

  public abstract long getNumOfBlocks();

  public abstract void setNumOfBlocksMissing(long blocks);

  public abstract long getNumOfBlocksMissing();

  public abstract void setNumOfBlocksPendingReplication(long blocks);

  public abstract long getNumOfBlocksPendingReplication();

  public abstract void setNumOfBlocksUnderReplicated(long blocks);

  public abstract long getNumOfBlocksUnderReplicated();

  public abstract void setNumOfBlocksPendingDeletion(long blocks);

  public abstract long getNumOfBlocksPendingDeletion();

  public abstract void setNumOfActiveDatanodes(int nodes);

  public abstract int getNumOfActiveDatanodes();

  public abstract void setNumOfDeadDatanodes(int nodes);

  public abstract int getNumOfDeadDatanodes();

  public abstract void setNumOfDecommissioningDatanodes(int nodes);

  public abstract int getNumOfDecommissioningDatanodes();

  public abstract void setNumOfDecomActiveDatanodes(int nodes);

  public abstract int getNumOfDecomActiveDatanodes();

  public abstract void setNumOfDecomDeadDatanodes(int nodes);

  public abstract int getNumOfDecomDeadDatanodes();

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    // This record is not stored directly, no key needed
    SortedMap<String, String> map = new TreeMap<String, String>();
    return map;
  }

  @Override
  public long getExpirationMs() {
    // This record is not stored directly, no expiration needed
    return -1;
  }

  @Override
  public void setDateModified(long time) {
    // We don't store this record directly
  }

  @Override
  public long getDateModified() {
    // We don't store this record directly
    return 0;
  }

  @Override
  public void setDateCreated(long time) {
    // We don't store this record directly
  }

  @Override
  public long getDateCreated() {
    // We don't store this record directly
    return 0;
  }
}