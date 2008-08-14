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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or 
 * other DataNodes.  This small server does not use the 
 * Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  
  ServerSocket ss;
  DataNode datanode;
  // Record all sockets opend for data transfer
  Map<Socket, Socket> childSockets = Collections.synchronizedMap(
                                       new HashMap<Socket, Socket>());
  
  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  static final int MAX_XCEIVER_COUNT = 256;
  int maxXceiverCount = MAX_XCEIVER_COUNT;

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
    
    this.maxXceiverCount = conf.getInt("dfs.datanode.max.xcievers",
        MAX_XCEIVER_COUNT);
    
    this.estimateBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
  }

  /**
   */
  public void run() {
    while (datanode.shouldRun) {
      try {
        Socket s = ss.accept();
        s.setTcpNoDelay(true);
        new Daemon(datanode.threadGroup, 
            new DataXceiver(s, datanode, this)).start();
      } catch (IOException ie) {
        LOG.warn(datanode.dnRegistration + ":DataXceiveServer: " 
                                + StringUtils.stringifyException(ie));
      } catch (Throwable te) {
        LOG.error(datanode.dnRegistration + ":DataXceiveServer: Exiting due to:" 
                                 + StringUtils.stringifyException(te));
        datanode.shouldRun = false;
      }
    }
    try {
      ss.close();
    } catch (IOException ie) {
      LOG.warn(datanode.dnRegistration + ":DataXceiveServer: " 
                              + StringUtils.stringifyException(ie));
    }
  }
  
  void kill() {
    assert datanode.shouldRun == false :
      "shoudRun should be set to false before killing";
    try {
      this.ss.close();
    } catch (IOException ie) {
      LOG.warn(datanode.dnRegistration + ":DataXceiveServer.kill(): " 
                              + StringUtils.stringifyException(ie));
    }

    // close all the sockets that were accepted earlier
    synchronized (childSockets) {
      for (Iterator<Socket> it = childSockets.values().iterator();
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
