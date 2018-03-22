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

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * Entry to track the version of the State Store data stored in the State Store
 * by a Router.
 */
public abstract class StateStoreVersion extends BaseRecord {

  public static StateStoreVersion newInstance() {
    return StateStoreSerializer.newRecord(StateStoreVersion.class);
  }

  public static StateStoreVersion newInstance(long membershipVersion,
      long mountTableVersion) {
    StateStoreVersion record = newInstance();
    record.setMembershipVersion(membershipVersion);
    record.setMountTableVersion(mountTableVersion);
    return record;
  }

  public abstract long getMembershipVersion();

  public abstract void setMembershipVersion(long version);

  public abstract long getMountTableVersion();

  public abstract void setMountTableVersion(long version);

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

  @Override
  public String toString() {
    return "Membership: " + getMembershipVersion() +
        " Mount Table: " + getMountTableVersion();
  }
}
