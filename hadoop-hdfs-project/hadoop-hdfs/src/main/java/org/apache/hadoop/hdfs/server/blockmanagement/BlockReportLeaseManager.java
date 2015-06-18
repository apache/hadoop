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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The BlockReportLeaseManager manages block report leases.<p/>
 *
 * DataNodes request BR leases from the NameNode by sending a heartbeat with
 * the requestBlockReportLease field set.  The NameNode may choose to respond
 * with a non-zero lease ID.  If so, that DataNode can send a block report with
 * the given lease ID for the next few minutes.  The NameNode will accept
 * these full block reports.<p/>
 *
 * BR leases limit the number of incoming full block reports to the NameNode
 * at any given time.  For compatibility reasons, the NN will always accept
 * block reports sent with a lease ID of 0 and queue them for processing
 * immediately.  Full block reports which were manually triggered will also
 * have a lease ID of 0, bypassing the rate-limiting.<p/>
 *
 * Block report leases expire after a certain amount of time.  This mechanism
 * is in place so that a DN which dies while holding a lease does not
 * permanently decrease the number of concurrent block reports which the NN is
 * willing to accept.<p/>
 *
 * When considering which DNs to grant a BR lease, the NameNode gives priority
 * to the DNs which have gone the longest without sending a full block
 * report.<p/>
 */
class BlockReportLeaseManager {
  static final Logger LOG =
      LoggerFactory.getLogger(BlockReportLeaseManager.class);

  private static class NodeData {
    /**
     * The UUID of the datanode.
     */
    final String datanodeUuid;

    /**
     * The lease ID, or 0 if there is no lease.
     */
    long leaseId;

    /**
     * The time when the lease was issued, or 0 if there is no lease.
     */
    long leaseTimeMs;

    /**
     * Previous element in the list.
     */
    NodeData prev;

    /**
     * Next element in the list.
     */
    NodeData next;

    static NodeData ListHead(String name) {
      NodeData node = new NodeData(name);
      node.next = node;
      node.prev = node;
      return node;
    }

    NodeData(String datanodeUuid) {
      this.datanodeUuid = datanodeUuid;
    }

    void removeSelf() {
      if (this.prev != null) {
        this.prev.next = this.next;
      }
      if (this.next != null) {
        this.next.prev = this.prev;
      }
      this.next = null;
      this.prev = null;
    }

    void addToEnd(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.prev = this.prev;
      node.next = this;
      this.prev.next = node;
      this.prev = node;
    }

    void addToBeginning(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.next = this.next;
      node.prev = this;
      this.next.prev = node;
      this.next = node;
    }
  }

  /**
   * List of datanodes which don't currently have block report leases.
   */
  private final NodeData deferredHead = NodeData.ListHead("deferredHead");

  /**
   * List of datanodes which currently have block report leases.
   */
  private final NodeData pendingHead = NodeData.ListHead("pendingHead");

  /**
   * Maps datanode UUIDs to NodeData.
   */
  private final HashMap<String, NodeData> nodes = new HashMap<>();

  /**
   * The current length of the pending list.
   */
  private int numPending = 0;

  /**
   * The maximum number of leases to hand out at any given time.
   */
  private final int maxPending;

  /**
   * The number of milliseconds after which a lease will expire.
   */
  private final long leaseExpiryMs;

  /**
   * The next ID we will use for a block report lease.
   */
  private long nextId = ThreadLocalRandom.current().nextLong();

  BlockReportLeaseManager(Configuration conf) {
    this(conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT),
        conf.getLong(
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT));
  }

  BlockReportLeaseManager(int maxPending, long leaseExpiryMs) {
    Preconditions.checkArgument(maxPending >= 1,
        "Cannot set the maximum number of block report leases to a " +
            "value less than 1.");
    this.maxPending = maxPending;
    Preconditions.checkArgument(leaseExpiryMs >= 1,
        "Cannot set full block report lease expiry period to a value " +
         "less than 1.");
    this.leaseExpiryMs = leaseExpiryMs;
  }

  /**
   * Get the next block report lease ID.  Any number is valid except 0.
   */
  private synchronized long getNextId() {
    long id;
    do {
      id = nextId++;
    } while (id == 0);
    return id;
  }

  public synchronized void register(DatanodeDescriptor dn) {
    registerNode(dn);
  }

  private synchronized NodeData registerNode(DatanodeDescriptor dn) {
    if (nodes.containsKey(dn.getDatanodeUuid())) {
      LOG.info("Can't register DN {} because it is already registered.",
          dn.getDatanodeUuid());
      return null;
    }
    NodeData node = new NodeData(dn.getDatanodeUuid());
    deferredHead.addToBeginning(node);
    nodes.put(dn.getDatanodeUuid(), node);
    LOG.info("Registered DN {} ({}).", dn.getDatanodeUuid(), dn.getXferAddr());
    return node;
  }

  private synchronized void remove(NodeData node) {
    if (node.leaseId != 0) {
      numPending--;
      node.leaseId = 0;
      node.leaseTimeMs = 0;
    }
    node.removeSelf();
  }

  public synchronized void unregister(DatanodeDescriptor dn) {
    NodeData node = nodes.remove(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't unregister DN {} because it is not currently " +
          "registered.", dn.getDatanodeUuid());
      return;
    }
    remove(node);
  }

  public synchronized long requestLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.warn("DN {} ({}) requested a lease even though it wasn't yet " +
          "registered.  Registering now.", dn.getDatanodeUuid(),
          dn.getXferAddr());
      node = registerNode(dn);
    }
    if (node.leaseId != 0) {
      // The DataNode wants a new lease, even though it already has one.
      // This can happen if the DataNode is restarted in between requesting
      // a lease and using it.
      LOG.debug("Removing existing BR lease 0x{} for DN {} in order to " +
               "issue a new one.", Long.toHexString(node.leaseId),
               dn.getDatanodeUuid());
    }
    remove(node);
    long monotonicNowMs = Time.monotonicNow();
    pruneExpiredPending(monotonicNowMs);
    if (numPending >= maxPending) {
      if (LOG.isDebugEnabled()) {
        StringBuilder allLeases = new StringBuilder();
        String prefix = "";
        for (NodeData cur = pendingHead.next; cur != pendingHead;
             cur = cur.next) {
          allLeases.append(prefix).append(cur.datanodeUuid);
          prefix = ", ";
        }
        LOG.debug("Can't create a new BR lease for DN {}, because " +
              "numPending equals maxPending at {}.  Current leases: {}",
              dn.getDatanodeUuid(), numPending, allLeases.toString());
      }
      return 0;
    }
    numPending++;
    node.leaseId = getNextId();
    node.leaseTimeMs = monotonicNowMs;
    pendingHead.addToEnd(node);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created a new BR lease 0x{} for DN {}.  numPending = {}",
          Long.toHexString(node.leaseId), dn.getDatanodeUuid(), numPending);
    }
    return node.leaseId;
  }

  private synchronized boolean pruneIfExpired(long monotonicNowMs,
                                              NodeData node) {
    if (monotonicNowMs < node.leaseTimeMs + leaseExpiryMs) {
      return false;
    }
    LOG.info("Removing expired block report lease 0x{} for DN {}.",
        Long.toHexString(node.leaseId), node.datanodeUuid);
    Preconditions.checkState(node.leaseId != 0);
    remove(node);
    deferredHead.addToBeginning(node);
    return true;
  }

  private synchronized void pruneExpiredPending(long monotonicNowMs) {
    NodeData cur = pendingHead.next;
    while (cur != pendingHead) {
      NodeData next = cur.next;
      if (!pruneIfExpired(monotonicNowMs, cur)) {
        return;
      }
      cur = next;
    }
    LOG.trace("No entries remaining in the pending list.");
  }

  public synchronized boolean checkLease(DatanodeDescriptor dn,
                                         long monotonicNowMs, long id) {
    if (id == 0) {
      LOG.debug("Datanode {} is using BR lease id 0x0 to bypass " +
          "rate-limiting.", dn.getDatanodeUuid());
      return true;
    }
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("BR lease 0x{} is not valid for unknown datanode {}",
          Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (node.leaseId == 0) {
      LOG.warn("BR lease 0x{} is not valid for DN {}, because the DN " +
               "is not in the pending set.",
               Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (pruneIfExpired(monotonicNowMs, node)) {
      LOG.warn("BR lease 0x{} is not valid for DN {}, because the lease " +
               "has expired.", Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (id != node.leaseId) {
      LOG.warn("BR lease 0x{} is not valid for DN {}.  Expected BR lease 0x{}.",
          Long.toHexString(id), dn.getDatanodeUuid(),
          Long.toHexString(node.leaseId));
      return false;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("BR lease 0x{} is valid for DN {}.",
          Long.toHexString(id), dn.getDatanodeUuid());
    }
    return true;
  }

  public synchronized long removeLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't remove lease for unknown datanode {}",
               dn.getDatanodeUuid());
      return 0;
    }
    long id = node.leaseId;
    if (id == 0) {
      LOG.debug("DN {} has no lease to remove.", dn.getDatanodeUuid());
      return 0;
    }
    remove(node);
    deferredHead.addToEnd(node);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed BR lease 0x{} for DN {}.  numPending = {}",
                Long.toHexString(id), dn.getDatanodeUuid(), numPending);
    }
    return id;
  }
}
