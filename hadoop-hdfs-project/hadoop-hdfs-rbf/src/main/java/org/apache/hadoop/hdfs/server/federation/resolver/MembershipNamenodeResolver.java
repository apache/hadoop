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
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.OBSERVER;
import static org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState.UNAVAILABLE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.DisabledNameserviceStore;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.RecordStore;
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
import org.apache.hadoop.net.NetUtils;
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
  /** Disabled Nameservice State Store interface. */
  private DisabledNameserviceStore disabledNameserviceInterface;

  /** Parent router ID. */
  private String routerId;

  /** Cached lookup of namenodes for nameservice. The keys are a pair of the nameservice
   * name and a boolean indicating if observer namenodes should be listed first.
   * If true, observer namenodes are listed first. If false, active namenodes are listed first.
   *  Invalidated on cache refresh. */
  private Map<Pair<String, Boolean>, List<? extends FederationNamenodeContext>> cacheNS;
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
      this.membershipInterface = getStoreInterface(MembershipStore.class);
    }
    return this.membershipInterface;
  }

  private synchronized DisabledNameserviceStore getDisabledNameserviceStore()
      throws IOException {
    if (this.disabledNameserviceInterface == null) {
      this.disabledNameserviceInterface =
          getStoreInterface(DisabledNameserviceStore.class);
    }
    return this.disabledNameserviceInterface;
  }

  private <T extends RecordStore<?>> T getStoreInterface(Class<T> clazz)
      throws IOException{
    T store = this.stateStore.getRegisteredRecordStore(clazz);
    if (store == null) {
      throw new IOException("State Store does not have an interface for " +
          clazz.getSimpleName());
    }
    return store;
  }

  @Override
  public boolean loadCache(boolean force) {
    // Our cache depends on the store, update it first
    try {
      MembershipStore membership = getMembershipStore();
      if (!membership.loadCache(force)) {
        return false;
      }
      DisabledNameserviceStore disabled = getDisabledNameserviceStore();
      if (!disabled.loadCache(force)) {
        return false;
      }
    } catch (IOException e) {
      LOG.error("Cannot update membership from the State Store", e);
    }

    // Force refresh of active NN cache
    cacheBP.clear();
    cacheNS.clear();
    return true;
  }

  @Override public void updateUnavailableNamenode(String nsId,
      InetSocketAddress address) throws IOException {
    updateNameNodeState(nsId, address, UNAVAILABLE);
  }

  @Override
  public void updateActiveNamenode(
      final String nsId, final InetSocketAddress address) throws IOException {
    updateNameNodeState(nsId, address, ACTIVE);
  }


  private void updateNameNodeState(final String nsId,
      final InetSocketAddress address, FederationNamenodeServiceState state)
      throws IOException {
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
                record.getNameserviceId(), record.getNamenodeId(), state);
        membership.updateNamenodeRegistration(updateRequest);

        cacheNS.remove(Pair.of(nsId, Boolean.TRUE));
        cacheNS.remove(Pair.of(nsId, Boolean.FALSE));
        // Invalidating the full cacheBp since getting the blockpool id from
        // namespace id is quite costly.
        cacheBP.clear();
      }
    } catch (StateStoreUnavailableException e) {
      LOG.error("Cannot update {} as active, State Store unavailable", address);
    }
  }

  /**
   * Try to shuffle the multiple observer namenodes if listObserversFirst is true.
   * @param inputNameNodes the input FederationNamenodeContext list. If listObserversFirst is true,
   *                       all observers will be placed at the front of the collection.
   * @param listObserversFirst true if we need to shuffle the multiple front observer namenodes.
   * @return a list of FederationNamenodeContext.
   * @param <T> a subclass of FederationNamenodeContext.
   */
  private <T extends FederationNamenodeContext> List<T> shuffleObserverNN(
      List<T> inputNameNodes, boolean listObserversFirst) {
    if (!listObserversFirst) {
      return inputNameNodes;
    }
    // Get Observers first.
    List<T> observerList = new ArrayList<>();
    for (T t : inputNameNodes) {
      if (t.getState() == OBSERVER) {
        observerList.add(t);
      } else {
        // The inputNameNodes are already sorted, so it can break
        // when the first non-observer is encountered.
        break;
      }
    }
    // Returns the inputNameNodes if no shuffle is required
    if (observerList.size() <= 1) {
      return inputNameNodes;
    }

    // Shuffle multiple Observers
    Collections.shuffle(observerList);

    List<T> ret = new ArrayList<>(inputNameNodes.size());
    ret.addAll(observerList);
    for (int i = observerList.size(); i < inputNameNodes.size(); i++) {
      ret.add(inputNameNodes.get(i));
    }
    return Collections.unmodifiableList(ret);
  }

  @Override
  public List<? extends FederationNamenodeContext> getNamenodesForNameserviceId(
      final String nsId, boolean listObserversFirst) throws IOException {

    List<? extends FederationNamenodeContext> ret = cacheNS.get(Pair.of(nsId, listObserversFirst));
    if (ret != null) {
      return shuffleObserverNN(ret, listObserversFirst);
    }

    // Not cached, generate the value
    final List<MembershipState> result;
    try {
      MembershipState partial = MembershipState.newInstance();
      partial.setNameserviceId(nsId);
      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance(partial);
      result = getRecentRegistrationForQuery(request, true,
          false, listObserversFirst);
    } catch (StateStoreUnavailableException e) {
      LOG.error("Cannot get active NN for {}, State Store unavailable", nsId);
      return null;
    }
    if (result == null || result.isEmpty()) {
      LOG.error("Cannot locate eligible NNs for {}", nsId);
      return null;
    }

    // Mark disabled name services
    try {
      Set<String> disabled =
          getDisabledNameserviceStore().getDisabledNameservices();
      if (disabled == null) {
        LOG.error("Cannot get disabled name services");
      } else {
        for (MembershipState nn : result) {
          if (disabled.contains(nn.getNameserviceId())) {
            nn.setState(FederationNamenodeServiceState.DISABLED);
          }
        }
      }
    } catch (StateStoreUnavailableException e) {
      LOG.error("Cannot get disabled name services, State Store unavailable");
    }

    // Cache the response
    ret = Collections.unmodifiableList(result);
    cacheNS.put(Pair.of(nsId, listObserversFirst), result);
    return ret;
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
            getRecentRegistrationForQuery(request, true, false, false);
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
        report.getClusterId(), report.getBlockPoolId(),
        NetUtils.normalizeIP2HostName(report.getRpcAddress()),
        report.getServiceAddress(), report.getLifelineAddress(),
        report.getWebScheme(), report.getWebAddress(), report.getState(),
        report.getSafemode());

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
      stats.setProvidedSpace(report.getProvidedSpace());
      stats.setNumOfDecommissioningDatanodes(
          report.getNumDecommissioningDatanodes());
      stats.setNumOfActiveDatanodes(report.getNumLiveDatanodes());
      stats.setNumOfDeadDatanodes(report.getNumDeadDatanodes());
      stats.setNumOfStaleDatanodes(report.getNumStaleDatanodes());
      stats.setNumOfDecomActiveDatanodes(report.getNumDecomLiveDatanodes());
      stats.setNumOfDecomDeadDatanodes(report.getNumDecomDeadDatanodes());
      stats.setNumOfInMaintenanceLiveDataNodes(
          report.getNumInMaintenanceLiveDataNodes());
      stats.setNumOfInMaintenanceDeadDataNodes(
          report.getNumInMaintenanceDeadDataNodes());
      stats.setNumOfEnteringMaintenanceDataNodes(
          report.getNumEnteringMaintenanceDataNodes());
      stats.setCorruptFilesCount(report.getCorruptFilesCount());
      stats.setScheduledReplicationBlocks(
          report.getScheduledReplicationBlocks());
      stats.setNumberOfMissingBlocksWithReplicationFactorOne(
          report.getNumberOfMissingBlocksWithReplicationFactorOne());
      stats.setHighestPriorityLowRedundancyReplicatedBlocks(
          report.getHighestPriorityLowRedundancyReplicatedBlocks());
      stats.setHighestPriorityLowRedundancyECBlocks(
          report.getHighestPriorityLowRedundancyECBlocks());
      stats.setPendingSPSPaths(report.getPendingSPSPaths());
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
    Set<FederationNamespaceInfo> nss = response.getNamespaceInfo();

    // Filter disabled namespaces
    Set<FederationNamespaceInfo> ret = new TreeSet<>();
    Set<String> disabled = getDisabledNamespaces();
    for (FederationNamespaceInfo ns : nss) {
      if (!disabled.contains(ns.getNameserviceId())) {
        ret.add(ns);
      }
    }

    return ret;
  }

  @Override
  public Set<String> getDisabledNamespaces() throws IOException {
    DisabledNameserviceStore store = getDisabledNameserviceStore();
    return store.getDisabledNameservices();
  }

  /**
   * Picks the most relevant record registration that matches the query.
   * If not observer read,
   * return registrations matching the query in this preference:
   * 1) Most recently updated ACTIVE registration
   * 2) Most recently updated Observer registration
   * 3) Most recently updated STANDBY registration (if showStandby)
   * 4) Most recently updated UNAVAILABLE registration (if showUnavailable).
   *
   * If observer read,
   * return registrations matching the query in this preference:
   * 1) Observer registrations, shuffled to disperse queries.
   * 2) Most recently updated ACTIVE registration
   * 3) Most recently updated STANDBY registration (if showStandby)
   * 4) Most recently updated UNAVAILABLE registration (if showUnavailable).
   *
   * EXPIRED registrations are ignored.
   *
   * @param request The select query for NN registrations.
   * @param addUnavailable include UNAVAILABLE registrations.
   * @param addExpired include EXPIRED registrations.
   * @param observerRead  Observer read case, observer NN will be ranked first
   * @return List of memberships or null if no registrations that
   *         both match the query AND the selected states.
   * @throws IOException
   */
  private List<MembershipState> getRecentRegistrationForQuery(
      GetNamenodeRegistrationsRequest request, boolean addUnavailable,
      boolean addExpired, boolean observerRead) throws IOException {

    // Retrieve a list of all registrations that match this query.
    // This may include all NN records for a namespace/blockpool, including
    // duplicate records for the same NN from different routers.
    MembershipStore membershipStore = getMembershipStore();
    GetNamenodeRegistrationsResponse response =
        membershipStore.getNamenodeRegistrations(request);

    List<MembershipState> memberships = response.getNamenodeMemberships();
    List<MembershipState> observerMemberships = new ArrayList<>();
    Iterator<MembershipState> iterator = memberships.iterator();
    while (iterator.hasNext()) {
      MembershipState membership = iterator.next();
      if (membership.getState() == EXPIRED && !addExpired) {
        iterator.remove();
      } else if (membership.getState() == UNAVAILABLE && !addUnavailable) {
        iterator.remove();
      } else if (membership.getState() == OBSERVER && observerRead) {
        iterator.remove();
        observerMemberships.add(membership);
      }
    }

    memberships.sort(new NamenodePriorityComparator());
    if(observerRead) {
      List<MembershipState> ret = new ArrayList<>(
          memberships.size() + observerMemberships.size());
      if(observerMemberships.size() > 1) {
        Collections.shuffle(observerMemberships);
      }
      ret.addAll(observerMemberships);
      ret.addAll(memberships);
      memberships = ret;
    }

    LOG.debug("Selected most recent NN {} for query", memberships);
    return memberships;
  }

  @Override
  public void setRouterId(String router) {
    this.routerId = router;
  }

  /**
   * Rotate cache, make the current namenode have the lowest priority,
   * to ensure that the current namenode will not be accessed first next time.
   *
   * @param nsId name service id.
   * @param namenode namenode contexts.
   * @param listObserversFirst Observer read case, observer NN will be ranked first.
   */
  @Override
  public void rotateCache(
      String nsId, FederationNamenodeContext namenode, boolean listObserversFirst) {
    cacheNS.compute(Pair.of(nsId, listObserversFirst), (ns, namenodeContexts) -> {
      if (namenodeContexts == null || namenodeContexts.size() <= 1) {
        return namenodeContexts;
      }

      // If there is active nn, rotateCache is not needed
      // because the router has already loaded the cache.
      for (FederationNamenodeContext namenodeContext : namenodeContexts) {
        if (namenodeContext.getState() == ACTIVE) {
          return namenodeContexts;
        }
      }

      // If the last namenode in the cache at this time
      // is the namenode whose priority needs to be lowered.
      // No need to rotate cache, because other threads have already rotated the cache.
      FederationNamenodeContext lastNamenode = namenodeContexts.get(namenodeContexts.size()-1);
      if (lastNamenode.getRpcAddress().equals(namenode.getRpcAddress())) {
        return namenodeContexts;
      }

      // Move the inaccessible namenode to the end of the cache,
      // to ensure that the namenode will not be accessed first next time.
      List<FederationNamenodeContext> rotateNamenodeContexts =
          (List<FederationNamenodeContext>) namenodeContexts;
      rotateNamenodeContexts.remove(namenode);
      rotateNamenodeContexts.add(namenode);
      LOG.info("Rotate cache of pair<{}, {}> -> {}",
          nsId, listObserversFirst, rotateNamenodeContexts);
      return rotateNamenodeContexts;
    });
  }
}
