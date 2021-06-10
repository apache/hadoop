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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.common.BlockAlias;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * This class allows us to manage and multiplex between storages local to
 * datanodes, and provided storage.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProvidedStorageMap {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProvidedStorageMap.class);

  // limit to a single provider for now
  private RwLock lock;
  private BlockManager bm;
  private BlockAliasMap aliasMap;

  private final String storageId;
  private final ProvidedDescriptor providedDescriptor;
  private final DatanodeStorageInfo providedStorageInfo;
  private boolean providedEnabled;
  private long capacity;
  private int defaultReplication;

  ProvidedStorageMap(RwLock lock, BlockManager bm, Configuration conf)
      throws IOException {

    storageId = conf.get(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);

    providedEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT);

    if (!providedEnabled) {
      // disable mapping
      aliasMap = null;
      providedDescriptor = null;
      providedStorageInfo = null;
      return;
    }

    DatanodeStorage ds = new DatanodeStorage(
        storageId, State.NORMAL, StorageType.PROVIDED);
    providedDescriptor = new ProvidedDescriptor();
    providedStorageInfo = providedDescriptor.createProvidedStorage(ds);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    this.bm = bm;
    this.lock = lock;

    // load block reader into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
            DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
            TextFileRegionAliasMap.class, BlockAliasMap.class);
    aliasMap = ReflectionUtils.newInstance(aliasMapClass, conf);

    LOG.info("Loaded alias map class: " +
        aliasMap.getClass() + " storage: " + providedStorageInfo);
  }

  /**
   * @param dn datanode descriptor
   * @param s data node storage
   * @return the {@link DatanodeStorageInfo} for the specified datanode.
   * If {@code s} corresponds to a provided storage, the storage info
   * representing provided storage is returned.
   * @throws IOException
   */
  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s)
      throws IOException {
    if (providedEnabled && storageId.equals(s.getStorageID())) {
      if (StorageType.PROVIDED.equals(s.getStorageType())) {
        if (providedStorageInfo.getState() == State.FAILED
            && s.getState() == State.NORMAL) {
          providedStorageInfo.setState(State.NORMAL);
          LOG.info("Provided storage transitioning to state " + State.NORMAL);
        }
        if (dn.getStorageInfo(s.getStorageID()) == null) {
          dn.injectStorage(providedStorageInfo);
        }
        processProvidedStorageReport();
        return providedDescriptor.getProvidedStorage(dn, s);
      }
      LOG.warn("Reserved storage {} reported as non-provided from {}", s, dn);
    }
    return dn.getStorageInfo(s.getStorageID());
  }

  private void processProvidedStorageReport()
      throws IOException {
    assert lock.hasWriteLock() : "Not holding write lock";
    if (providedStorageInfo.getBlockReportCount() == 0
        || providedDescriptor.activeProvidedDatanodes() == 0) {
      LOG.info("Calling process first blk report from storage: "
          + providedStorageInfo);
      // first pass; periodic refresh should call bm.processReport
      BlockAliasMap.Reader<BlockAlias> reader =
          aliasMap.getReader(null, bm.getBlockPoolId());
      if (reader != null) {
        bm.processFirstBlockReport(providedStorageInfo,
                new ProvidedBlockList(reader.iterator()));
      }
    }
  }

  @VisibleForTesting
  public DatanodeStorageInfo getProvidedStorageInfo() {
    return providedStorageInfo;
  }

  public LocatedBlockBuilder newLocatedBlocks(int maxValue) {
    if (!providedEnabled) {
      return new LocatedBlockBuilder(maxValue);
    }
    return new ProvidedBlocksBuilder(maxValue);
  }

  public void removeDatanode(DatanodeDescriptor dnToRemove) {
    if (providedEnabled) {
      assert lock.hasWriteLock() : "Not holding write lock";
      providedDescriptor.remove(dnToRemove);
      // if all datanodes fail, set the block report count to 0
      if (providedDescriptor.activeProvidedDatanodes() == 0) {
        providedStorageInfo.setBlockReportCount(0);
      }
    }
  }

  public long getCapacity() {
    if (providedStorageInfo == null) {
      return 0;
    }
    return providedStorageInfo.getCapacity();
  }

  public void updateStorage(DatanodeDescriptor node, DatanodeStorage storage) {
    if (isProvidedStorage(storage.getStorageID())) {
      if (StorageType.PROVIDED.equals(storage.getStorageType())) {
        node.injectStorage(providedStorageInfo);
        return;
      } else {
        LOG.warn("Reserved storage {} reported as non-provided from {}",
            storage, node);
      }
    }
    node.updateStorage(storage);
  }

  private boolean isProvidedStorage(String dnStorageId) {
    return providedEnabled && storageId.equals(dnStorageId);
  }

  /**
   * Choose a datanode that reported a volume of {@link StorageType} PROVIDED.
   *
   * @return the {@link DatanodeDescriptor} corresponding to a datanode that
   *         reported a volume with {@link StorageType} PROVIDED. If multiple
   *         datanodes report a PROVIDED volume, one is chosen uniformly at
   *         random.
   */
  public DatanodeDescriptor chooseProvidedDatanode() {
    return providedDescriptor.chooseRandom();
  }

  @VisibleForTesting
  public BlockAliasMap getAliasMap() {
    return aliasMap;
  }

  /**
   * Builder used for creating {@link LocatedBlocks} when a block is provided.
   */
  class ProvidedBlocksBuilder extends LocatedBlockBuilder {

    ProvidedBlocksBuilder(int maxBlocks) {
      super(maxBlocks);
    }

    private DatanodeDescriptor chooseProvidedDatanode(
        Set<String> excludedUUids) {
      DatanodeDescriptor dn = providedDescriptor.choose(null, excludedUUids);
      if (dn == null) {
        dn = providedDescriptor.choose(null);
      }
      return dn;
    }

    @Override
    LocatedBlock newLocatedBlock(ExtendedBlock eb,
        DatanodeStorageInfo[] storages, long pos, boolean isCorrupt) {

      List<DatanodeInfoWithStorage> locs = new ArrayList<>();
      List<String> sids = new ArrayList<>();
      List<StorageType> types = new ArrayList<>();
      boolean isProvidedBlock = false;
      Set<String> excludedUUids = new HashSet<>();

      for (int i = 0; i < storages.length; ++i) {
        DatanodeStorageInfo currInfo = storages[i];
        StorageType storageType = currInfo.getStorageType();
        sids.add(currInfo.getStorageID());
        types.add(storageType);
        if (StorageType.PROVIDED.equals(storageType)) {
          // Provided location will be added to the list of locations after
          // examining all local locations.
          isProvidedBlock = true;
        } else {
          locs.add(new DatanodeInfoWithStorage(
              currInfo.getDatanodeDescriptor(),
              currInfo.getStorageID(), storageType));
          excludedUUids.add(currInfo.getDatanodeDescriptor().getDatanodeUuid());
        }
      }

      int numLocations = locs.size();
      if (isProvidedBlock) {
        // add the first datanode here
        DatanodeDescriptor dn = chooseProvidedDatanode(excludedUUids);
        locs.add(
            new DatanodeInfoWithStorage(dn, storageId, StorageType.PROVIDED));
        excludedUUids.add(dn.getDatanodeUuid());
        numLocations++;
        // add more replicas until we reach the defaultReplication
        for (int count = numLocations + 1;
            count <= defaultReplication && count <= providedDescriptor
                .activeProvidedDatanodes(); count++) {
          dn = chooseProvidedDatanode(excludedUUids);
          locs.add(new DatanodeInfoWithStorage(
              dn, storageId, StorageType.PROVIDED));
          sids.add(storageId);
          types.add(StorageType.PROVIDED);
          excludedUUids.add(dn.getDatanodeUuid());
        }
      }
      return new LocatedBlock(eb,
          locs.toArray(new DatanodeInfoWithStorage[locs.size()]),
          sids.toArray(new String[sids.size()]),
          types.toArray(new StorageType[types.size()]),
          pos, isCorrupt, null);
    }

    @Override
    LocatedBlocks build(DatanodeDescriptor client) {
      // TODO choose provided locations close to the client.
      return new LocatedBlocks(
          flen, isUC, blocks, last, lastComplete, feInfo, ecPolicy);
    }

    @Override
    LocatedBlocks build() {
      return build(providedDescriptor.chooseRandom());
    }
  }

  /**
   * An abstract DatanodeDescriptor to track datanodes with provided storages.
   * NOTE: never resolved through registerDatanode, so not in the topology.
   */
  public static class ProvidedDescriptor extends DatanodeDescriptor {

    private final NavigableMap<String, DatanodeDescriptor> dns =
        new ConcurrentSkipListMap<>();
    // maintain a separate list of the datanodes with provided storage
    // to efficiently choose Datanodes when required.
    private final List<DatanodeDescriptor> dnR = new ArrayList<>();
    public final static String NETWORK_LOCATION = "/REMOTE";
    public final static String NAME = "PROVIDED";

    ProvidedDescriptor() {
      super(new DatanodeID(
            null,                         // String ipAddr,
            null,                         // String hostName,
            UUID.randomUUID().toString(), // String datanodeUuid,
            0,                            // int xferPort,
            0,                            // int infoPort,
            0,                            // int infoSecurePort,
            0));                          // int ipcPort
    }

    DatanodeStorageInfo getProvidedStorage(
        DatanodeDescriptor dn, DatanodeStorage s) {
      dns.put(dn.getDatanodeUuid(), dn);
      dnR.add(dn);
      return storageMap.get(s.getStorageID());
    }

    DatanodeStorageInfo createProvidedStorage(DatanodeStorage ds) {
      assert null == storageMap.get(ds.getStorageID());
      DatanodeStorageInfo storage = new ProvidedDatanodeStorageInfo(this, ds);
      storage.setHeartbeatedSinceFailover(true);
      storageMap.put(storage.getStorageID(), storage);
      return storage;
    }

    DatanodeDescriptor choose(DatanodeDescriptor client) {
      return choose(client, Collections.<String>emptySet());
    }

    DatanodeDescriptor choose(DatanodeDescriptor client,
        Set<String> excludedUUids) {
      // exact match for now
      if (client != null && !excludedUUids.contains(client.getDatanodeUuid())) {
        DatanodeDescriptor dn = dns.get(client.getDatanodeUuid());
        if (dn != null) {
          return dn;
        }
      }
      // prefer live nodes first.
      DatanodeDescriptor dn = chooseRandomNode(excludedUUids, true);
      if (dn == null) {
        dn = chooseRandomNode(excludedUUids, false);
      }
      return dn;
    }

    private DatanodeDescriptor chooseRandomNode(Set<String> excludedUUids,
        boolean preferLiveNodes) {
      Random r = new Random();
      for (int i = dnR.size() - 1; i >= 0; --i) {
        int pos = r.nextInt(i + 1);
        DatanodeDescriptor node = dnR.get(pos);
        String uuid = node.getDatanodeUuid();
        if (!excludedUUids.contains(uuid)) {
          if (!preferLiveNodes || node.getAdminState() == AdminStates.NORMAL) {
            return node;
          }
        }
        Collections.swap(dnR, i, pos);
      }
      return null;
    }

    DatanodeDescriptor chooseRandom(DatanodeStorageInfo... excludedStorages) {
      Set<String> excludedNodes = new HashSet<>();
      if (excludedStorages != null) {
        for (int i = 0; i < excludedStorages.length; i++) {
          DatanodeDescriptor dn = excludedStorages[i].getDatanodeDescriptor();
          String uuid = dn.getDatanodeUuid();
          excludedNodes.add(uuid);
        }
      }
      return choose(null, excludedNodes);
    }

    @Override
    public void addBlockToBeReplicated(Block block,
        DatanodeStorageInfo[] targets) {
      // pick a random datanode, delegate to it
      DatanodeDescriptor node = chooseRandom(targets);
      if (node != null) {
        node.addBlockToBeReplicated(block, targets);
      } else {
        LOG.error("Cannot find a source node to replicate block: "
            + block + " from");
      }
    }

    int remove(DatanodeDescriptor dnToRemove) {
      // this operation happens under the FSNamesystem lock;
      // no additional synchronization required.
      if (dnToRemove != null) {
        DatanodeDescriptor storedDN = dns.get(dnToRemove.getDatanodeUuid());
        if (storedDN != null) {
          dns.remove(dnToRemove.getDatanodeUuid());
          dnR.remove(dnToRemove);
        }
      }
      return dns.size();
    }

    int activeProvidedDatanodes() {
      return dns.size();
    }

    @Override
    public boolean equals(Object obj) {
      return (this == obj) || super.equals(obj);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public String toString() {
      return "PROVIDED-LOCATION";
    }

    @Override
    public String getNetworkLocation() {
      return NETWORK_LOCATION;
    }

    @Override
    public String getName() {
      return NAME;
    }
  }

  /**
   * The DatanodeStorageInfo used for the provided storage.
   */
  static class ProvidedDatanodeStorageInfo extends DatanodeStorageInfo {

    ProvidedDatanodeStorageInfo(ProvidedDescriptor dn, DatanodeStorage ds) {
      super(dn, ds);
    }

    @Override
    boolean removeBlock(BlockInfo b) {
      ProvidedDescriptor dn = (ProvidedDescriptor) getDatanodeDescriptor();
      if (dn.activeProvidedDatanodes() == 0) {
        return super.removeBlock(b);
      } else {
        return false;
      }
    }

    @Override
    void setState(DatanodeStorage.State state) {
      if (state == State.FAILED) {
        // The state should change to FAILED only when there are no active
        // datanodes with PROVIDED storage.
        ProvidedDescriptor dn = (ProvidedDescriptor) getDatanodeDescriptor();
        if (dn.activeProvidedDatanodes() == 0) {
          LOG.info("Provided storage {} transitioning to state {}",
              this, State.FAILED);
          super.setState(state);
        }
      } else {
        super.setState(state);
      }
    }

    @Override
    public String toString() {
      return "PROVIDED-STORAGE";
    }
  }

  /**
   * Used to emulate block reports for provided blocks.
   */
  static class ProvidedBlockList extends BlockListAsLongs {

    private final Iterator<BlockAlias> inner;

    ProvidedBlockList(Iterator<BlockAlias> inner) {
      this.inner = inner;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        @Override
        public BlockReportReplica next() {
          return new BlockReportReplica(inner.next().getBlock());
        }
        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumberOfBlocks() {
      // is ignored for ProvidedBlockList.
      return -1;
    }

    @Override
    public ByteString getBlocksBuffer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getBlockListAsLongs() {
      // should only be used for backwards compat, DN.ver > NN.ver
      throw new UnsupportedOperationException();
    }
  }
}
