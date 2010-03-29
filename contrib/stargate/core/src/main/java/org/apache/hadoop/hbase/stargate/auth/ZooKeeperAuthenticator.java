/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.auth;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.stargate.Constants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.stargate.User;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.json.JSONObject;

/**
 * A simple authenticator module for ZooKeeper.
 * <pre>
 *   /stargate/
 *     users/
 *       &lt;token&gt;</pre> 
 * Where <tt>&lt;token&gt;</tt> is a JSON formatted user record with the keys
 * 'name' (String, required), 'token' (String, optional), 'admin' (boolean,
 * optional), and 'disabled' (boolean, optional).
 */
public class ZooKeeperAuthenticator extends Authenticator 
    implements Constants {

  final String usersZNode;
  ZooKeeperWrapper wrapper;

  private boolean ensureParentExists(final String znode) {
    int index = znode.lastIndexOf("/");
    if (index <= 0) {   // Parent is root, which always exists.
      return true;
    }
    return ensureExists(znode.substring(0, index));
  }

  private boolean ensureExists(final String znode) {
    ZooKeeper zk = wrapper.getZooKeeper();
    try {
      Stat stat = zk.exists(znode, false);
      if (stat != null) {
        return true;
      }
      zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, 
        CreateMode.PERSISTENT);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return true;      // ok, move on.
    } catch (KeeperException.NoNodeException e) {
      return ensureParentExists(znode) && ensureExists(znode);
    } catch (KeeperException e) {
    } catch (InterruptedException e) {
    }
    return false;
  }

  /**
   * Constructor
   * @param conf
   * @throws IOException
   */
  public ZooKeeperAuthenticator(Configuration conf) throws IOException {
    this(conf, new ZooKeeperWrapper(conf, new Watcher() {
      public void process(WatchedEvent event) { }
    }));
    ensureExists(USERS_ZNODE_ROOT);
  }

  /**
   * Constructor
   * @param conf
   * @param wrapper
   */
  public ZooKeeperAuthenticator(Configuration conf, 
      ZooKeeperWrapper wrapper) {
    this.usersZNode = conf.get("stargate.auth.zk.users", USERS_ZNODE_ROOT);
    this.wrapper = wrapper;
  }

  @Override
  public User getUserForToken(String token) throws IOException {
    ZooKeeper zk = wrapper.getZooKeeper();
    try {
      byte[] data = zk.getData(usersZNode + "/" + token, null, null);
      if (data == null) {
        return null;
      }
      JSONObject o = new JSONObject(Bytes.toString(data));
      if (!o.has("name")) {
        throw new IOException("invalid record, missing 'name'");
      }
      String name = o.getString("name");
      boolean admin = false;
      if (o.has("admin")) { admin = o.getBoolean("admin"); }
      boolean disabled = false;
      if (o.has("disabled")) { disabled = o.getBoolean("disabled"); }
      return new User(name, token, admin, disabled);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
