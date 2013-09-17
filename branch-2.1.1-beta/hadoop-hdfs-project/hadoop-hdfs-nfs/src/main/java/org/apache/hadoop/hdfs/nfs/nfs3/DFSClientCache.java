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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A cache saves DFSClient objects for different users
 */
public class DFSClientCache {
  static final Log LOG = LogFactory.getLog(DFSClientCache.class);
  private final LruCache<String, DFSClient> lruTable;
  private final Configuration config;

  public DFSClientCache(Configuration config) {
    // By default, keep 256 DFSClient instance for 256 active users
    this(config, 256);
  }

  public DFSClientCache(Configuration config, int size) {
    lruTable = new LruCache<String, DFSClient>(size);
    this.config = config;
  }

  public void put(String uname, DFSClient client) {
    lruTable.put(uname, client);
  }

  synchronized public DFSClient get(String uname) {
    DFSClient client = lruTable.get(uname);
    if (client != null) {
      return client;
    }

    // Not in table, create one.
    try {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(uname);
      client = ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
        public DFSClient run() throws IOException {
          return new DFSClient(NameNode.getAddress(config), config);
        }
      });
    } catch (IOException e) {
      LOG.error("Create DFSClient failed for user:" + uname);
      e.printStackTrace();

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // Add new entry
    lruTable.put(uname, client);
    return client;
  }

  public int usedSize() {
    return lruTable.usedSize();
  }

  public boolean containsKey(String key) {
    return lruTable.containsKey(key);
  }
}
