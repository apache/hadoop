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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;


/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or 
 * other DataNodes.  This small server does not use the 
 * Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable {
  public static final Log LOG = DataNode.LOG;
  
  ServerSocket ss;
  DataNode datanode;
  // Record all sockets opened for data transfer
  Set<Socket> childSockets = Collections.synchronizedSet(
                                       new HashSet<Socket>());
  
  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  int maxXceiverCount =
    DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;

  /** A manager to make sure that cluster balancing does not
   * take too much resources.
   * 
   * It limits the number of block moves for balancing and
   * the total amount of bandwidth they can use.
   */
  static class BlockBalanceThrottler extends DataTransferThrottler {
   private int numThreads;
   
   /**Constructor
    * 
    * @param bandwidth Total amount of bandwidth can be used for balancing 
    */
   private BlockBalanceThrottler(long bandwidth) {
     super(bandwidth);
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
   }
   
   /** Check if the block move can start. 
    * 
    * Return true if the thread quota is not exceeded and 
    * the counter is incremented; False otherwise.
    */
   synchronized boolean acquire() {
     if (numThreads >= Balancer.MAX_NUM_CONCURRENT_MOVES) {
       return false;
     }
     numThreads++;
     return true;
   }
   
   /** Mark that the move is completed. The thread counter is decremented. */
   synchronized void release() {
     numThreads--;
   }
  }

  BlockBalanceThrottler balanceThrottler;
  
  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. For now we set it to be the default block size set
   * in the server side configuration, which is not ideal because the
   * default block size should be a client-size configuration. 
   * A better solution is to include in the header the estimated block size,
   * i.e. either the actual block size or the default block size.
   */
  long estimateBlockSize;
  
  
  DataXceiverServer(ServerSocket ss, Configuration conf, 
      DataNode datanode) {
    
    this.ss = ss;
    this.datanode = datanode;
    
    this.maxXceiverCount = 
      conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                  DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);
    
    this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    
    //set up parameter for cluster balancing
    this.balanceThrottler = new BlockBalanceThrottler(
      conf.getLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY, 
                   DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT));
  }

  @Override
  public void run() {
    while (datanode.shouldRun) {
      Socket s = null;
      try {
        s = ss.accept();
        s.setTcpNoDelay(true);
        // Timeouts are set within DataXceiver.run()

        // Make sure the xceiver count is not exceeded
        int curXceiverCount = datanode.getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("Xceiver count " + curXceiverCount
              + " exceeds the limit of concurrent xcievers: "
              + maxXceiverCount);
        }

        new Daemon(datanode.threadGroup,
            DataXceiver.create(s, datanode, this))
            .start();
      } catch (SocketTimeoutException ignored) {
        // wake up to see if should continue to run
      } catch (AsynchronousCloseException ace) {
        // another thread closed our listener socket - that's expected during shutdown,
        // but not in other circumstances
        if (datanode.shouldRun) {
          LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ace);
        }
      } catch (IOException ie) {
        IOUtils.closeSocket(s);
        LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ie);
      } catch (OutOfMemoryError ie) {
        IOUtils.closeSocket(s);
        // DataNode can run out of memory if there is too many transfers.
        // Log the event, Sleep for 30 seconds, other transfers may complete by
        // then.
        LOG.warn("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error(datanode.getDisplayName()
            + ":DataXceiverServer: Exiting due to: ", te);
        datanode.shouldRun = false;
      }
    }
    try {
      ss.close();
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName()
          + " :DataXceiverServer: close exception", ie);
    }
  }
  
  void kill() {
    assert datanode.shouldRun == false :
      "shoudRun should be set to false before killing";
    try {
      this.ss.close();
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName() + ":DataXceiverServer.kill(): ", ie);
    }

    // close all the sockets that were accepted earlier
    synchronized (childSockets) {
      for (Iterator<Socket> it = childSockets.iterator();
           it.hasNext();) {
        Socket thissock = it.next();
        try {
          thissock.close();
        } catch (IOException e) {
        }
      }
    }
  }
}
