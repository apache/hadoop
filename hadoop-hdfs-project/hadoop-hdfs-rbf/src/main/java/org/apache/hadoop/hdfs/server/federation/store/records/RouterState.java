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

import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry to log the state of a
 * {@link org.apache.hadoop.hdfs.server.federation.router.Router Router} in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.StateStoreService
 * FederationStateStoreService}.
 */
public abstract class RouterState extends BaseRecord {

  private static final Logger LOG = LoggerFactory.getLogger(RouterState.class);

  /** Expiration time in ms for this entry. */
  private static long expirationMs;

  /**
   * Constructors.
   */
  public RouterState() {
    super();
  }

  public static RouterState newInstance() {
    RouterState record = StateStoreSerializer.newRecord(RouterState.class);
    record.init();
    return record;
  }

  public static RouterState newInstance(String addr, long startTime,
      RouterServiceState status) {
    RouterState record = newInstance();
    record.setDateStarted(startTime);
    record.setAddress(addr);
    record.setStatus(status);
    record.setCompileInfo(FederationUtil.getCompileInfo());
    record.setVersion(FederationUtil.getVersion());
    return record;
  }

  public abstract void setAddress(String address);

  public abstract void setDateStarted(long dateStarted);

  public abstract String getAddress();

  public abstract StateStoreVersion getStateStoreVersion() throws IOException;

  public abstract void setStateStoreVersion(StateStoreVersion version);

  public abstract RouterServiceState getStatus();

  public abstract void setStatus(RouterServiceState newStatus);

  public abstract String getVersion();

  public abstract void setVersion(String version);

  public abstract String getCompileInfo();

  public abstract void setCompileInfo(String info);

  public abstract long getDateStarted();

  /**
   * Get the identifier for the Router. It uses the address.
   *
   * @return Identifier for the Router.
   */
  public String getRouterId() {
    return getAddress();
  }

  @Override
  public boolean like(BaseRecord o) {
    if (o instanceof RouterState) {
      RouterState other = (RouterState)o;
      if (getAddress() != null &&
          !getAddress().equals(other.getAddress())) {
        return false;
      }
      if (getStatus() != null &&
          !getStatus().equals(other.getStatus())) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return getAddress() + " -> " + getStatus() + "," + getVersion();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("address", getAddress());
    return map;
  }

  @Override
  public void validate() {
    super.validate();
    if ((getAddress() == null || getAddress().length() == 0) &&
        getStatus() != RouterServiceState.INITIALIZING) {
      throw new IllegalArgumentException(
          "Invalid router entry, no address specified " + this);
    }
  }

  @Override
  public int compareTo(BaseRecord other) {
    if (other == null) {
      return -1;
    } else if (other instanceof RouterState) {
      RouterState router = (RouterState) other;
      return this.getAddress().compareTo(router.getAddress());
    } else {
      return super.compareTo(other);
    }
  }

  @Override
  public boolean checkExpired(long currentTime) {
    if (super.checkExpired(currentTime)) {
      setStatus(RouterServiceState.EXPIRED);
      return true;
    }
    return false;
  }

  @Override
  public long getExpirationMs() {
    return RouterState.expirationMs;
  }

  public static void setExpirationMs(long time) {
    RouterState.expirationMs = time;
  }
}
