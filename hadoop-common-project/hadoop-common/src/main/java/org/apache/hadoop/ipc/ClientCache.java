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

package org.apache.hadoop.ipc;

import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/* Cache a client using its socket factory as the hash key */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ClientCache {
  private Map<SocketFactory, Client> clients =
    new HashMap<SocketFactory, Client>();

  /**
   * Construct &amp; cache an IPC client with the user-provided SocketFactory
   * if no cached client exists.
   * 
   * @param conf Configuration
   * @param factory SocketFactory for client socket
   * @param valueClass Class of the expected response
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf,
      SocketFactory factory, Class<? extends Writable> valueClass) {
    // Construct & cache client.  The configuration is only used for timeout,
    // and Clients have connection pools.  So we can either (a) lose some
    // connection pooling and leak sockets, or (b) use the same timeout for all
    // configurations.  Since the IPC is usually intended globally, not
    // per-job, we choose (a).
    Client client = clients.get(factory);
    if (client == null) {
      client = new Client(valueClass, conf, factory);
      clients.put(factory, client);
    } else {
      client.incCount();
    }
    if (Client.LOG.isDebugEnabled()) {
      Client.LOG.debug("getting client out of cache: " + client);
    }
    return client;
  }

  /**
   * Construct &amp; cache an IPC client with the default SocketFactory
   * and default valueClass if no cached client exists. 
   * 
   * @param conf Configuration
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf) {
    return getClient(conf, SocketFactory.getDefault(), ObjectWritable.class);
  }
  
  /**
   * Construct &amp; cache an IPC client with the user-provided SocketFactory
   * if no cached client exists. Default response type is ObjectWritable.
   * 
   * @param conf Configuration
   * @param factory SocketFactory for client socket
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf, SocketFactory factory) {
    return this.getClient(conf, factory, ObjectWritable.class);
  }

  /**
   * Stop a RPC client connection 
   * A RPC client is closed only when its reference count becomes zero.
   */
  public void stopClient(Client client) {
    if (Client.LOG.isDebugEnabled()) {
      Client.LOG.debug("stopping client from cache: " + client);
    }
    final int count;
    synchronized (this) {
      count = client.decAndGetCount();
      if (count == 0) {
        if (Client.LOG.isDebugEnabled()) {
          Client.LOG.debug("removing client from cache: " + client);
        }
        clients.remove(client.getSocketFactory());
      }
    }
    if (count == 0) {
      if (Client.LOG.isDebugEnabled()) {
        Client.LOG.debug("stopping actual client because no more references remain: "
            + client);
      }
      client.stop();
    }
  }
}
