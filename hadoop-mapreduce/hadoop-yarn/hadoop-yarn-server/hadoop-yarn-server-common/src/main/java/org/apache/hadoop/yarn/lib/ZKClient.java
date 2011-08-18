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

package org.apache.hadoop.yarn.lib;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/** ZK Registration Library
 * currently does not use any authorization
 */
public class ZKClient {
  private ZooKeeper zkClient;
  
  /**
   * the zookeeper client library to 
   * talk to zookeeper 
   * @param string the host
   * @throws throws IOException
   */
  public ZKClient(String string) throws IOException {
    zkClient = new ZooKeeper(string, 30000, new ZKWatcher());
  }
  
  /**
   * register the service to a specific path
   * @param path the path in zookeeper namespace to register to
   * @param data the data that is part of this registration
   * @throws IOException
   * @throws InterruptedException
   */
  public void registerService(String path, String data) throws
    IOException, InterruptedException {
    try {
      zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, 
          CreateMode.EPHEMERAL);
    } catch(KeeperException ke) {
      throw new IOException(ke);
    }
  }
  
  /**
   * unregister the service. 
   * @param path the path at which the service was registered
   * @throws IOException
   * @throws InterruptedException
   */
  public void unregisterService(String path) throws IOException,
    InterruptedException {
    try {
      zkClient.delete(path, -1);
    } catch(KeeperException ke) {
      throw new IOException(ke);
    }
  }

  /**
   * list the services registered under a path
   * @param path the path under which services are
   * registered
   * @return the list of names of services registered
   * @throws IOException 
   * @throws InterruptedException
   */
  public List<String> listServices(String path) throws IOException, 
    InterruptedException {
    List<String> children = null;
    try {
      children = zkClient.getChildren(path, false);
    } catch(KeeperException ke) {
      throw new IOException(ke);
    }
    return children;
  }
  
  /**
   * get data published by the service at the registration address
   * @param path the path where the service is registered 
   * @return  the data of the registered service
   * @throws IOException
   * @throws InterruptedException
   */
  public String getServiceData(String path) throws IOException,
    InterruptedException {
    String data;
    try {
      Stat stat = new Stat();
      byte[] byteData = zkClient.getData(path, false, stat);
      data = new String(byteData);
    } catch(KeeperException ke) {
      throw new IOException(ke);
    }
    return data;
  }
  
  
  /**
   * a watcher class that handles what events from
   * zookeeper.
   *
   */
  private static class ZKWatcher implements Watcher {

    @Override
    public void process(WatchedEvent arg0) {
      
    }
    
  }
}
