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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;

/**
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Logger LOG = DataNode.LOG;
  
  private final Map<String, BPOfferService> bpByNameserviceId =
    Maps.newHashMap();
  private final Map<String, BPOfferService> bpByBlockPoolId =
    Maps.newHashMap();
  private final List<BPOfferService> offerServices =
      new CopyOnWriteArrayList<>();

  private final DataNode dn;

  //This lock is used only to ensure exclusion of refreshNamenodes
  private final Object refreshNamenodesLock = new Object();
  
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions.checkArgument(offerServices.contains(bpos),
        "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns a list of BPOfferService objects. The underlying list
   * implementation is a CopyOnWriteArrayList so it can be safely
   * iterated while BPOfferServices are being added or removed.
   *
   * Caution: The BPOfferService returned could be shutdown any time.
   */
  synchronized List<BPOfferService> getAllNamenodeThreads() {
    return Collections.unmodifiableList(offerServices);
  }
      
  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  synchronized void remove(BPOfferService t) {
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // It's possible that the block pool never successfully registered
      // with any NN, so it was never added it to this map
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed;) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  void shutDownAll(List<BPOfferService> bposList) throws InterruptedException {
    for (BPOfferService bpos : bposList) {
      bpos.stop(); //interrupts the threads
    }
    //now join
    for (BPOfferService bpos : bposList) {
      bpos.join();
    }
  }
  
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              for (BPOfferService bpos : offerServices) {
                bpos.start();
              }
              return null;
            }
          });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  void joinAll() {
    for (BPOfferService bpos: this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }
  
  void refreshNamenodes(Configuration conf)
      throws IOException {
    LOG.info("Refresh request received for nameservices: " + conf.get
            (DFSConfigKeys.DFS_NAMESERVICES));

    Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil
            .getNNServiceRpcAddressesForCluster(conf);
    Map<String, Map<String, InetSocketAddress>> newLifelineAddressMap = DFSUtil
            .getNNLifelineRpcAddressesForCluster(conf);

    synchronized (refreshNamenodesLock) {
      doRefreshNamenodes(newAddressMap, newLifelineAddressMap);
    }
  }
  
  private void doRefreshNamenodes(
      Map<String, Map<String, InetSocketAddress>> addrMap,
      Map<String, Map<String, InetSocketAddress>> lifelineAddrMap)
      throws IOException {
    assert Thread.holdsLock(refreshNamenodesLock);

    Set<String> toRefresh = Sets.newLinkedHashSet();
    Set<String> toAdd = Sets.newLinkedHashSet();
    Set<String> toRemove;
    
    synchronized (this) {
      // Step 1. For each of the new nameservices, figure out whether
      // it's an update of the set of NNs for an existing NS,
      // or an entirely new nameservice.
      for (String nameserviceId : addrMap.keySet()) {
        if (bpByNameserviceId.containsKey(nameserviceId)) {
          toRefresh.add(nameserviceId);
        } else {
          toAdd.add(nameserviceId);
        }
      }
      
      // Step 2. Any nameservices we currently have but are no longer present
      // need to be removed.
      toRemove = Sets.newHashSet(Sets.difference(
          bpByNameserviceId.keySet(), addrMap.keySet()));
      
      assert toRefresh.size() + toAdd.size() ==
        addrMap.size() :
          "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
          "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
          "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

      
      // Step 3. Start new nameservices
      if (!toAdd.isEmpty()) {
        LOG.info("Starting BPOfferServices for nameservices: " +
            Joiner.on(",").useForNull("<default>").join(toAdd));
      
        for (String nsToAdd : toAdd) {
          Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToAdd);
          Map<String, InetSocketAddress> nnIdToLifelineAddr =
              lifelineAddrMap.get(nsToAdd);
          ArrayList<InetSocketAddress> addrs =
              Lists.newArrayListWithCapacity(nnIdToAddr.size());
          ArrayList<InetSocketAddress> lifelineAddrs =
              Lists.newArrayListWithCapacity(nnIdToAddr.size());
          for (String nnId : nnIdToAddr.keySet()) {
            addrs.add(nnIdToAddr.get(nnId));
            lifelineAddrs.add(nnIdToLifelineAddr != null ?
                nnIdToLifelineAddr.get(nnId) : null);
          }
          BPOfferService bpos = createBPOS(nsToAdd, addrs, lifelineAddrs);
          bpByNameserviceId.put(nsToAdd, bpos);
          offerServices.add(bpos);
        }
      }
      startAll();
    }

    // Step 4. Shut down old nameservices. This happens outside
    // of the synchronized(this) lock since they need to call
    // back to .remove() from another thread
    if (!toRemove.isEmpty()) {
      LOG.info("Stopping BPOfferServices for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRemove));
      
      for (String nsToRemove : toRemove) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
        bpos.stop();
        bpos.join();
        // they will call remove on their own
      }
    }
    
    // Step 5. Update nameservices whose NN list has changed
    if (!toRefresh.isEmpty()) {
      LOG.info("Refreshing list of NNs for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRefresh));
      
      for (String nsToRefresh : toRefresh) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
        Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToRefresh);
        Map<String, InetSocketAddress> nnIdToLifelineAddr =
            lifelineAddrMap.get(nsToRefresh);
        ArrayList<InetSocketAddress> addrs =
            Lists.newArrayListWithCapacity(nnIdToAddr.size());
        ArrayList<InetSocketAddress> lifelineAddrs =
            Lists.newArrayListWithCapacity(nnIdToAddr.size());
        for (String nnId : nnIdToAddr.keySet()) {
          addrs.add(nnIdToAddr.get(nnId));
          lifelineAddrs.add(nnIdToLifelineAddr != null ?
              nnIdToLifelineAddr.get(nnId) : null);
        }
        bpos.refreshNNList(addrs, lifelineAddrs);
      }
    }
  }

  /**
   * Extracted out for test purposes.
   */
  protected BPOfferService createBPOS(
      final String nameserviceId,
      List<InetSocketAddress> nnAddrs,
      List<InetSocketAddress> lifelineNnAddrs) {
    return new BPOfferService(nameserviceId, nnAddrs, lifelineNnAddrs, dn);
  }
}
