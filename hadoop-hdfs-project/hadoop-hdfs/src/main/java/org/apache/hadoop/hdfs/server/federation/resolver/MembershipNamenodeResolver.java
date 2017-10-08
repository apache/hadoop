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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.ACTIVE;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.EXPIRED;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.UNAVAILABLE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreCache;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a cached lookup of the most recently active namenode for a
 * particular nameservice. Relies on the {@link StateStoreService} to
 * discover available nameservices and namenodes.
 */
public class MembershipNamenodeResolver
    implements ActiveNamenodeResolver, StateStoreCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(MembershipNamenodeResolver.class);

  /** Reference to the State Store. */
  private final StateStoreService stateStore;
  /** Membership State Store interface. */
  private MembershipStore membershipInterface;

  /** Parent router ID. */
  private String routerId;

  /** Cached lookup of NN for nameservice. Invalidated on cache refresh. */
  private Map<String, List<? extends FederationNamenodeContext>> cacheNS;
  /** Cached lookup of NN for block pool. Invalidated on cache refresh. */
  private Map<String, List<? extends FederationNamenodeContext>> cacheBP;


  public MembershipNamenodeResolver(
      Configuration conf, StateStoreService store) throws IOException {
    this.stateStore = store;

    this.cacheNS = new ConcurrentHashMap<>();
    this.cacheBP = new ConcurrentHashMap<>();

    if (this.stateStore != null) {
      // Request cache updates from the state store
      this.stateStore.registerCacheExternal(this);
    }
  }

  private synchronized MembershipStore getMembershipStore() throws IOException {
    if (this.membershipInterface == null) {
      this.membershipInterface = this.stateStore.getRegisteredRecordStore(
          MembershipStore.class);
      if (this.membershipInterface == null) {
        throw new IOException("State Store does not have an interface for " +
            MembershipStore.class.getSimpleName());
      }
    }
    return this.membershipInterface;
  }

  @Override
  public boolean loadCache(boolean force) {
    // Our cache depends on the store, update it first
    try {
      MembershipStore membership = getMembershipStore();
      membership.loadCache(force);
    } catch (IOException e) {
      LOG.error("Cannot update membership from the State Store", e);
    }

    // Force refresh of active NN cache
    cacheBP.clear();
    cacheNS.clear();
    return true;
  }

  @Override
  public void updateActiveNamenode(
      final String nsId, final InetSocketAddress address) throws IOException {

    // Called when we have an RPC miss and successful hit on an alternate NN.
    // Temporarily update our cache, it will be overwritten on the next update.
    try {
      MembershipState partial = MembershipState.newInstance();
      String rpcAddress = address.getHostName() + ":" + address.getPort();
      partial.setRpcAddress(rpcAddress);
      partial.setNameserviceId(nsId);

      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance(partial);

      MembershipStore membership = getMembershipStore();
      GetNamenodeRegistrationsResponse response =
          membership.getNamenodeRegistrations(request);
      List<MembershipState> records = response.getNamenodeMemberships();

      if (records != null && records.size() == 1) {
        MembershipState record = records.get(0);
        UpdateNamenodeRegistrationRequest updateRequest =
            UpdateNamenodeRegistrationRequest.newInstance(
                record.getNameserviceId(), record.getNamenodeId(), ACTIVE);
        membership.updateNamenodeRegistration(updateRequest);
      }
    } catch (StateStoreUnavailableException e) {
      LOG.error("Cannot update {} as active, State Store unavailable", address);
    }
  }

  @Override
  public List<? extends FederationNamenodeContext> getNamenodesForNameserviceId(
      final String nsId) throws IOException {

    List<? extends FederationNamenodeContext> ret = cacheNS.get(nsId);
    if (ret == null) {
      try {
        MembershipState partial = MembershipState.newInstance();
        partial.setNameserviceId(nsId);
        GetNamenodeRegistrationsRequest request =
            GetNamenodeRegistrationsRequest.newInstance(partial);

        final List<MembershipState> result =
            getRecentRegistrationForQuery(request, true, false);
        if (result == null || result.isEmpty()) {
          LOG.error("Cannot locate eligible NNs for {}", nsId);
          return null;
        } else {
          cacheNS.put(nsId, result);
          ret = result;
        }
      } catch (StateStoreUnavailableException e) {
        LOG.error("Cannot get active NN for {}, State Store unavailable", nsId);
      }
    }
    if (ret == null) {
      return null;
    }
    return Collections.unmodifiableList(ret);
  }

  @Override
  public List<? extends FederationNamenodeContext> getNamenodesForBlockPoolId(
      final String bpId) throws IOException {

    List<? extends FederationNamenodeContext> ret = cacheBP.get(bpId);
    if (ret == null) {
      try {
        MembershipState partial = MembershipState.newInstance();
        partial.setBlockPoolId(bpId);
        GetNamenodeRegistrationsRequest request =
            GetNamenodeRegistrationsRequest.newInstance(partial);

        final List<MembershipState> result =
            getRecentRegistrationForQuery(request, true, false);
        if (result == null || result.isEmpty()) {
          LOG.error("Cannot locate eligible NNs for {}", bpId);
        } else {
          cacheBP.put(bpId, result);
          ret = result;
        }
      } catch (StateStoreUnavailableException e) {
        LOG.error("Cannot get active NN for {}, State Store unavailable", bpId);
        return null;
      }
    }
    if (ret == null) {
      return null;
    }
    return Collections.unmodifiableList(ret);
  }

  @Override
  public boolean registerNamenode(NamenodeStatusReport report)
      throws IOException {

    if (this.routerId == null) {
      LOG.warn("Cannot register namenode, router ID is not known {}", report);
      return false;
    }

    MembershipState record = MembershipState.newInstance(
        routerId, report.getNameserviceId(), report.getNamenodeId(),
        report.getClusterId(), report.getBlockPoolId(), report.getRpcAddress(),
        report.getServiceAddress(), report.getLifelineAddress(),
        report.getWebAddress(), report.getState(), report.getSafemode());

    if (report.statsValid()) {
      MembershipStats stats = MembershipStats.newInstance();
      stats.setNumOfFiles(report.getNumFiles());
      stats.setNumOfBlocks(report.getNumBlocks());
      stats.setNumOfBlocksMissing(report.getNumBlocksMissing());
      stats.setNumOfBlocksPendingReplication(
          report.getNumOfBlocksPendingReplication());
      stats.setNumOfBlocksUnderReplicated(
          report.getNumOfBlocksUnderReplicated());
      stats.setNumOfBlocksPendingDeletion(
          report.getNumOfBlocksPendingDeletion());
      stats.setAvailableSpace(report.getAvailableSpace());
      stats.setTotalSpace(report.getTotalSpace());
      stats.setNumOfDecommissioningDatanodes(
          report.getNumDecommissioningDatanodes());
      stats.setNumOfActiveDatanodes(report.getNumLiveDatanodes());
      stats.setNumOfDeadDatanodes(report.getNumDeadDatanodes());
      stats.setNumOfDecomActiveDatanodes(report.getNumDecomLiveDatanodes());
      stats.setNumOfDecomDeadDatanodes(report.getNumDecomDeadDatanodes());
      record.setStats(stats);
    }

    if (report.getState() != UNAVAILABLE) {
      // Set/update our last contact time
      record.setLastContact(Time.now());
    }

    NamenodeHeartbeatRequest request = NamenodeHeartbeatRequest.newInstance();
    request.setNamenodeMembership(record);
    return getMembershipStore().namenodeHeartbeat(request).getResult();
  }

  @Override
  public Set<FederationNamespaceInfo> getNamespaces() throws IOException {
    GetNamespaceInfoRequest request = GetNamespaceInfoRequest.newInstance();
    GetNamespaceInfoResponse response =
        getMembershipStore().getNamespaceInfo(request);
    return response.getNamespaceInfo();
  }

  /**
   * Picks the most relevant record registration that matches the query. Return
   * registrations matching the query in this preference: 1) Most recently
   * updated ACTIVE registration 2) Most recently updated STANDBY registration
   * (if showStandby) 3) Most recently updated UNAVAILABLE registration (if
   * showUnavailable). EXPIRED registrations are ignored.
   *
   * @param query The select query for NN registrations.
   * @param excludes List of NNs to exclude from matching results.
   * @param addUnavailable include UNAVAILABLE registrations.
   * @param addExpired include EXPIRED registrations.
   * @return List of memberships or null if no registrations that
   *         both match the query AND the selected states.
   * @throws IOException
   */
  private List<MembershipState> getRecentRegistrationForQuery(
      GetNamenodeRegistrationsRequest request, boolean addUnavailable,
      boolean addExpired) throws IOException {

    // Retrieve a list of all registrations that match this query.
    // This may include all NN records for a namespace/blockpool, including
    // duplicate records for the same NN from different routers.
    MembershipStore membershipStore = getMembershipStore();
    GetNamenodeRegistrationsResponse response =
        membershipStore.getNamenodeRegistrations(request);

    List<MembershipState> memberships = response.getNamenodeMemberships();
    if (!addExpired || !addUnavailable) {
      Iterator<MembershipState> iterator = memberships.iterator();
      while (iterator.hasNext()) {
        MembershipState membership = iterator.next();
        if (membership.getState() == EXPIRED && !addExpired) {
          iterator.remove();
        } else if (membership.getState() == UNAVAILABLE && !addUnavailable) {
          iterator.remove();
        }
      }
    }

    List<MembershipState> priorityList = new ArrayList<>();
    priorityList.addAll(memberships);
    Collections.sort(priorityList, new NamenodePriorityComparator());

    LOG.debug("Selected most recent NN {} for query", priorityList);
    return priorityList;
  }

  @Override
  public void setRouterId(String router) {
    this.routerId = router;
  }
}
