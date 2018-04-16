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

import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.ACTIVE;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.EXPIRED;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.UNAVAILABLE;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * Data schema for storing NN registration information in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.StateStoreService
 * FederationStateStoreService}.
 */
public abstract class MembershipState extends BaseRecord
    implements FederationNamenodeContext {
  public static final String ERROR_MSG_NO_NS_SPECIFIED =
      "Invalid registration, no nameservice specified ";
  public static final String ERROR_MSG_NO_WEB_ADDR_SPECIFIED =
      "Invalid registration, no web address specified ";
  public static final String ERROR_MSG_NO_RPC_ADDR_SPECIFIED =
      "Invalid registration, no rpc address specified ";
  public static final String ERROR_MSG_NO_BP_SPECIFIED =
      "Invalid registration, no block pool specified ";

  /** Expiration time in ms for this entry. */
  private static long expirationMs;


  /** Comparator based on the name.*/
  public static final Comparator<MembershipState> NAME_COMPARATOR =
      new Comparator<MembershipState>() {
        public int compare(MembershipState m1, MembershipState m2) {
          return m1.compareNameTo(m2);
        }
      };


  /**
   * Constructors.
   */
  public MembershipState() {
    super();
  }

  /**
   * Create a new membership instance.
   * @return Membership instance.
   * @throws IOException
   */
  public static MembershipState newInstance() {
    MembershipState record =
        StateStoreSerializer.newRecord(MembershipState.class);
    record.init();
    return record;
  }

  /**
   * Create a new membership instance.
   *
   * @param router Identifier of the router.
   * @param nameservice Identifier of the nameservice.
   * @param namenode Identifier of the namenode.
   * @param clusterId Identifier of the cluster.
   * @param blockPoolId Identifier of the blockpool.
   * @param rpcAddress RPC address.
   * @param serviceAddress Service RPC address.
   * @param lifelineAddress Lifeline RPC address.
   * @param webAddress HTTP address.
   * @param state State of the federation.
   * @param safemode If the safe mode is enabled.
   * @return Membership instance.
   * @throws IOException If we cannot create the instance.
   */
  public static MembershipState newInstance(String router, String nameservice,
      String namenode, String clusterId, String blockPoolId, String rpcAddress,
      String serviceAddress, String lifelineAddress, String webAddress,
      FederationNamenodeServiceState state, boolean safemode) {

    MembershipState record = MembershipState.newInstance();
    record.setRouterId(router);
    record.setNameserviceId(nameservice);
    record.setNamenodeId(namenode);
    record.setRpcAddress(rpcAddress);
    record.setServiceAddress(serviceAddress);
    record.setLifelineAddress(lifelineAddress);
    record.setWebAddress(webAddress);
    record.setIsSafeMode(safemode);
    record.setState(state);
    record.setClusterId(clusterId);
    record.setBlockPoolId(blockPoolId);
    record.validate();
    return record;
  }

  public abstract void setRouterId(String routerId);

  public abstract String getRouterId();

  public abstract void setNameserviceId(String nameserviceId);

  public abstract void setNamenodeId(String namenodeId);

  public abstract void setWebAddress(String webAddress);

  public abstract void setRpcAddress(String rpcAddress);

  public abstract void setServiceAddress(String serviceAddress);

  public abstract void setLifelineAddress(String lifelineAddress);

  public abstract void setIsSafeMode(boolean isSafeMode);

  public abstract void setClusterId(String clusterId);

  public abstract void setBlockPoolId(String blockPoolId);

  public abstract void setState(FederationNamenodeServiceState state);

  public abstract String getNameserviceId();

  public abstract String getNamenodeId();

  public abstract String getClusterId();

  public abstract String getBlockPoolId();

  public abstract String getRpcAddress();

  public abstract String getServiceAddress();

  public abstract String getLifelineAddress();

  public abstract String getWebAddress();

  public abstract boolean getIsSafeMode();

  public abstract FederationNamenodeServiceState getState();

  public abstract void setStats(MembershipStats stats);

  public abstract MembershipStats getStats();

  public abstract void setLastContact(long contact);

  public abstract long getLastContact();

  @Override
  public boolean like(BaseRecord o) {
    if (o instanceof MembershipState) {
      MembershipState other = (MembershipState)o;
      if (getRouterId() != null &&
          !getRouterId().equals(other.getRouterId())) {
        return false;
      }
      if (getNameserviceId() != null &&
          !getNameserviceId().equals(other.getNameserviceId())) {
        return false;
      }
      if (getNamenodeId() != null &&
          !getNamenodeId().equals(other.getNamenodeId())) {
        return false;
      }
      if (getRpcAddress() != null &&
          !getRpcAddress().equals(other.getRpcAddress())) {
        return false;
      }
      if (getClusterId() != null &&
          !getClusterId().equals(other.getClusterId())) {
        return false;
      }
      if (getBlockPoolId() != null &&
          !getBlockPoolId().equals(other.getBlockPoolId())) {
        return false;
      }
      if (getState() != null &&
          !getState().equals(other.getState())) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return getRouterId() + "->" + getNameserviceId() + ":" + getNamenodeId()
        + ":" + getRpcAddress() + "-" + getState();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("routerId", getRouterId());
    map.put("nameserviceId", getNameserviceId());
    map.put("namenodeId", getNamenodeId());
    return map;
  }

  /**
   * Check if the namenode is available.
   *
   * @return If the namenode is available.
   */
  public boolean isAvailable() {
    return getState() == ACTIVE;
  }

  /**
   * Validates the entry. Throws an IllegalArgementException if the data record
   * is missing required information.
   */
  @Override
  public void validate() {
    super.validate();
    if (getNameserviceId() == null || getNameserviceId().length() == 0) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_NS_SPECIFIED + this);
    }
    if (getWebAddress() == null || getWebAddress().length() == 0) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_WEB_ADDR_SPECIFIED + this);
    }
    if (getRpcAddress() == null || getRpcAddress().length() == 0) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_RPC_ADDR_SPECIFIED + this);
    }
    if (!isBadState() &&
        (getBlockPoolId().isEmpty() || getBlockPoolId().length() == 0)) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_BP_SPECIFIED + this);
    }
  }


  /**
   * Overrides the cached getBlockPoolId() with an update. The state will be
   * reset when the cache is flushed
   *
   * @param newState Service state of the namenode.
   */
  public void overrideState(FederationNamenodeServiceState newState) {
    this.setState(newState);
  }

  /**
   * Sort by nameservice, namenode, and router.
   *
   * @param other Another membership to compare to.
   * @return If this object goes before the parameter.
   */
  public int compareNameTo(MembershipState other) {
    int ret = this.getNameserviceId().compareTo(other.getNameserviceId());
    if (ret == 0) {
      ret = this.getNamenodeId().compareTo(other.getNamenodeId());
    }
    if (ret == 0) {
      ret = this.getRouterId().compareTo(other.getRouterId());
    }
    return ret;
  }

  /**
   * Get the identifier of this namenode registration.
   * @return Identifier of the namenode.
   */
  public String getNamenodeKey() {
    return getNamenodeKey(this.getNameserviceId(), this.getNamenodeId());
  }

  /**
   * Generate the identifier for a Namenode in the HDFS federation.
   *
   * @param nsId Nameservice of the Namenode.
   * @param nnId Namenode within the Nameservice (HA).
   * @return Namenode identifier within the federation.
   */
  public static String getNamenodeKey(String nsId, String nnId) {
    return nsId + "-" + nnId;
  }

  /**
   * Check if the membership is in a bad state (expired or unavailable).
   * @return If the membership is in a bad state (expired or unavailable).
   */
  private boolean isBadState() {
    return this.getState() == EXPIRED || this.getState() == UNAVAILABLE;
  }

  @Override
  public boolean checkExpired(long currentTime) {
    if (super.checkExpired(currentTime)) {
      this.setState(EXPIRED);
      // Commit it
      return true;
    }
    return false;
  }

  @Override
  public long getExpirationMs() {
    return MembershipState.expirationMs;
  }

  /**
   * Set the expiration time for this class.
   *
   * @param time Expiration time in milliseconds.
   */
  public static void setExpirationMs(long time) {
    MembershipState.expirationMs = time;
  }
}
