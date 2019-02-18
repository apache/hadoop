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
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;

/**
 * Server used for receiving/sending a block of data. This is created to listen
 * for requests from clients or other DataNodes. This small server does not use
 * the Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable {
  public static final Logger LOG = DataNode.LOG;

  /**
   * Default time to wait (in seconds) for the number of running threads to drop
   * below the newly requested maximum before giving up.
   */
  private static final int DEFAULT_RECONFIGURE_WAIT = 30;

  private final PeerServer peerServer;
  private final DataNode datanode;
  private final HashMap<Peer, Thread> peers = new HashMap<>();
  private final HashMap<Peer, DataXceiver> peersXceiver = new HashMap<>();
  private final Lock lock = new ReentrantLock();
  private final Condition noPeers = lock.newCondition();
  private boolean closed = false;
  private int maxReconfigureWaitTime = DEFAULT_RECONFIGURE_WAIT;

  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  int maxXceiverCount =
    DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;

  /**
   * A manager to make sure that cluster balancing does not take too much
   * resources.
   *
   * It limits the number of block moves for balancing and the total amount of
   * bandwidth they can use.
   */
  static class BlockBalanceThrottler extends DataTransferThrottler {
    private final Semaphore semaphore;
    private int maxThreads;

   /**
    * Constructor.
    *
    * @param bandwidth Total amount of bandwidth can be used for balancing
    */
    private BlockBalanceThrottler(long bandwidth, int maxThreads) {
      super(bandwidth);
      this.semaphore = new Semaphore(maxThreads, true);
      this.maxThreads = maxThreads;
      LOG.info("Balancing bandwidth is " + bandwidth + " bytes/s");
      LOG.info("Number threads for balancing is " + maxThreads);
    }

    /**
     * Update the number of threads which may be used concurrently for moving
     * blocks. The number of threads available can be scaled up or down. If
     * increasing the number of threads, the request will be serviced
     * immediately. However, if decreasing the number of threads, this method
     * will block any new request for moves, wait for any existing backlog of
     * move requests to clear, and wait for enough threads to have finished such
     * that the total number of threads actively running is less than or equal
     * to the new cap. If this method has been unable to successfully set the
     * new, lower, cap within 'duration' seconds, the attempt will be aborted
     * and the original cap will remain.
     *
     * @param newMaxThreads The new maximum number of threads for block moving
     * @param duration The number of seconds to wait if decreasing threads
     * @return true if new maximum was successfully applied; false otherwise
     */
    private boolean setMaxConcurrentMovers(final int newMaxThreads,
        final int duration) {
      Preconditions.checkArgument(newMaxThreads > 0);
      final int delta = newMaxThreads - this.maxThreads;
      LOG.debug("Change concurrent thread count to {} from {}", newMaxThreads,
          this.maxThreads);
      if (delta == 0) {
        return true;
      }
      if (delta > 0) {
        LOG.debug("Adding thread capacity: {}", delta);
        this.semaphore.release(delta);
        this.maxThreads = newMaxThreads;
        return true;
      }
      try {
        LOG.debug("Removing thread capacity: {}. Max wait: {}", delta,
            duration);
        boolean acquired = this.semaphore.tryAcquire(Math.abs(delta), duration,
            TimeUnit.SECONDS);
        if (acquired) {
          this.maxThreads = newMaxThreads;
        } else {
          LOG.warn("Could not lower thread count to {} from {}. Too busy.",
              newMaxThreads, this.maxThreads);
        }
        return acquired;
      } catch (InterruptedException e) {
        LOG.warn("Interrupted before adjusting thread count: {}", delta);
        return false;
      }
    }

    @VisibleForTesting
    int getMaxConcurrentMovers() {
      return this.maxThreads;
    }

   /**
    * Check if the block move can start
    *
    * Return true if the thread quota is not exceeded and
    * the counter is incremented; False otherwise.
    */
    boolean acquire() {
      return this.semaphore.tryAcquire();
    }

    /**
     * Mark that the move is completed. The thread counter is decremented.
     */
    void release() {
      this.semaphore.release();
    }
  }

  final BlockBalanceThrottler balanceThrottler;

  /**
   * Stores an estimate for block size to check if the disk partition has enough
   * space. Newer clients pass the expected block size to the DataNode. For
   * older clients, just use the server-side default block size.
   */
  final long estimateBlockSize;

  DataXceiverServer(PeerServer peerServer, Configuration conf,
      DataNode datanode) {
    this.peerServer = peerServer;
    this.datanode = datanode;

    this.maxXceiverCount =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                  DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);

    this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    //set up parameter for cluster balancing
    this.balanceThrottler = new BlockBalanceThrottler(
        conf.getLongBytes(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT));
  }

  @Override
  public void run() {
    Peer peer = null;
    while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
      try {
        peer = peerServer.accept();

        // Make sure the xceiver count is not exceeded
        int curXceiverCount = datanode.getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("Xceiver count " + curXceiverCount
              + " exceeds the limit of concurrent xcievers: "
              + maxXceiverCount);
        }

        new Daemon(datanode.threadGroup,
            DataXceiver.create(peer, datanode, this))
            .start();
      } catch (SocketTimeoutException ignored) {
        // wake up to see if should continue to run
      } catch (AsynchronousCloseException ace) {
        // another thread closed our listener socket - that's expected during shutdown,
        // but not in other circumstances
        if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
          LOG.warn("{}:DataXceiverServer", datanode.getDisplayName(), ace);
        }
      } catch (IOException ie) {
        IOUtils.closeQuietly(peer);
        LOG.warn("{}:DataXceiverServer", datanode.getDisplayName(), ie);
      } catch (OutOfMemoryError ie) {
        IOUtils.closeQuietly(peer);
        // DataNode can run out of memory if there is too many transfers.
        // Log the event, Sleep for 30 seconds, other transfers may complete by
        // then.
        LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error("{}:DataXceiverServer: Exiting.", datanode.getDisplayName(),
            te);
        datanode.shouldRun = false;
      }
    }

    // Close the server to stop reception of more requests.
    lock.lock();
    try {
      if (!closed) {
        peerServer.close();
        closed = true;
      }
    } catch (IOException ie) {
      LOG.warn("{}:DataXceiverServer: close exception",
          datanode.getDisplayName(), ie);
    } finally {
      lock.unlock();
    }

    // if in restart prep stage, notify peers before closing them.
    if (datanode.shutdownForUpgrade) {
      restartNotifyPeers();
      // Each thread needs some time to process it. If a thread needs
      // to send an OOB message to the client, but blocked on network for
      // long time, we need to force its termination.
      LOG.info("Shutting down DataXceiverServer before restart");

      waitAllPeers(2L, TimeUnit.SECONDS);
    }

    closeAllPeers();
  }

  void kill() {
    assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    lock.lock();
    try {
      if (!closed) {
        peerServer.close();
        closed = true;
      }
    } catch (IOException ie) {
      LOG.warn("{}:DataXceiverServer.kill()", datanode.getDisplayName(), ie);
    } finally {
      lock.unlock();
    }
  }

  void addPeer(Peer peer, Thread t, DataXceiver xceiver)
      throws IOException {
    lock.lock();
    try {
      if (closed) {
        throw new IOException("Server closed.");
      }
      peers.put(peer, t);
      peersXceiver.put(peer, xceiver);
      datanode.metrics.incrDataNodeActiveXceiversCount();
    } finally {
      lock.unlock();
    }
  }

  void closePeer(Peer peer) {
    lock.lock();
    try {
      peers.remove(peer);
      peersXceiver.remove(peer);
      datanode.metrics.decrDataNodeActiveXceiversCount();
      IOUtils.closeQuietly(peer);
      if (peers.isEmpty()) {
        this.noPeers.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  // Sending OOB to all peers
  public void sendOOBToPeers() {
    lock.lock();
    try {
      if (!datanode.shutdownForUpgrade) {
        return;
      }
      for (Peer p : peers.keySet()) {
        try {
          peersXceiver.get(p).sendOOB();
        } catch (IOException e) {
          LOG.warn("Got error when sending OOB message.", e);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted when sending OOB message.");
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void stopWriters() {
    lock.lock();
    try {
      peers.keySet().forEach(p -> peersXceiver.get(p).stopWriter());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Notify all peers of the shutdown and restart. 'datanode.shouldRun' should
   * still be true and 'datanode.restarting' should be set true before calling
   * this method.
   */
  void restartNotifyPeers() {
    assert (datanode.shouldRun && datanode.shutdownForUpgrade);
    lock.lock();
    try {
      // interrupt each and every DataXceiver thread.
      peers.values().forEach(t -> t.interrupt());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close all peers and clear the map.
   */
  void closeAllPeers() {
    LOG.info("Closing all peers.");
    lock.lock();
    try {
      peers.keySet().forEach(p -> IOUtils.closeQuietly(p));
      peers.clear();
      peersXceiver.clear();
      datanode.metrics.setDataNodeActiveXceiversCount(0);
      this.noPeers.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Causes a thread to block until all peers are removed, a certain amount of
   * time has passed, or the thread is interrupted.
   *
   * @param timeout the maximum time to wait, in nanoseconds
   * @param unit the unit of time to wait
   * @return true if thread returned because all peers were removed; false
   *         otherwise
   */
  private boolean waitAllPeers(long timeout, TimeUnit unit) {
    long nanos = unit.toNanos(timeout);
    lock.lock();
    try {
      while (!peers.isEmpty()) {
        if (nanos <= 0L) {
          return false;
        }
        nanos = noPeers.awaitNanos(nanos);
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted waiting for peers to close");
      return false;
    } finally {
      lock.unlock();
    }
    return true;
  }

  /**
   * Return the number of peers.
   *
   * @return the number of active peers
   */
  int getNumPeers() {
    lock.lock();
    try {
      return peers.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return the number of peers and DataXceivers.
   *
   * @return the number of peers and DataXceivers.
   */
  @VisibleForTesting
  int getNumPeersXceiver() {
    lock.lock();
    try {
      return peersXceiver.size();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  PeerServer getPeerServer() {
    return peerServer;
  }

  /**
   * Release a peer.
   *
   * @param peer The peer to release
   */
  void releasePeer(Peer peer) {
    lock.lock();
    try {
      peers.remove(peer);
      peersXceiver.remove(peer);
      datanode.metrics.decrDataNodeActiveXceiversCount();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update the number of threads which may be used concurrently for moving
   * blocks.
   *
   * @param movers The new maximum number of threads for block moving
   * @return true if new maximum was successfully applied; false otherwise
   */
  public boolean updateBalancerMaxConcurrentMovers(final int movers) {
    return balanceThrottler.setMaxConcurrentMovers(movers,
        this.maxReconfigureWaitTime);
  }

  /**
   * Update the maximum amount of time to wait for reconfiguration of the
   * maximum number of block mover threads to complete.
   *
   * @param max The new maximum number of threads for block moving, in seconds
   */
  @VisibleForTesting
  void setMaxReconfigureWaitTime(int max) {
    this.maxReconfigureWaitTime = max;
  }
}
