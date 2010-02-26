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

import org.apache.hadoop.hbase.stargate.MiniClusterTestBase;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import org.json.JSONStringer;

public class TestZooKeeperAuthenticator extends MiniClusterTestBase {

  static final String UNKNOWN_TOKEN = "00000000000000000000000000000000";
  static final String ADMIN_TOKEN = "e998efffc67c49c6e14921229a51b7b3";
  static final String ADMIN_USERNAME = "testAdmin";
  static final String USER_TOKEN = "da4829144e3a2febd909a6e1b4ed7cfa";
  static final String USER_USERNAME = "testUser";
  static final String DISABLED_TOKEN = "17de5b5db0fd3de0847bd95396f36d92";
  static final String DISABLED_USERNAME = "disabledUser";

  ZooKeeperAuthenticator authenticator;

  @Override
  public void setUp() throws Exception {
    authenticator = new ZooKeeperAuthenticator(conf);
    ZooKeeper zk = authenticator.wrapper.getZooKeeper();
    if (zk.exists(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" + 
          ADMIN_TOKEN, null) == null) {
      zk.create(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" + ADMIN_TOKEN, 
        Bytes.toBytes(new JSONStringer()
          .object()
            .key("name").value(ADMIN_USERNAME)
            .key("admin").value(true)
          .endObject().toString()),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" + 
          USER_TOKEN, null) == null) {
      zk.create(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" + USER_TOKEN, 
        Bytes.toBytes(new JSONStringer()
          .object()
            .key("name").value(USER_USERNAME)
            .key("disabled").value(false)
          .endObject().toString()),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" + 
          DISABLED_TOKEN, null) == null) {
      zk.create(ZooKeeperAuthenticator.USERS_ROOT_ZNODE + "/" +DISABLED_TOKEN,
        Bytes.toBytes(new JSONStringer()
          .object()
            .key("name").value(DISABLED_USERNAME)
            .key("admin").value(false)
            .key("disabled").value(true)
          .endObject().toString()),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  public void testGetUserUnknown() throws Exception {
    User user = authenticator.getUserForToken(UNKNOWN_TOKEN);
    assertNull(user);
  }

  public void testGetAdminUser() throws Exception {
    User user = authenticator.getUserForToken(ADMIN_TOKEN);
    assertNotNull(user);
    assertEquals(user.getName(), ADMIN_USERNAME);
    assertTrue(user.isAdmin());
    assertFalse(user.isDisabled());
  }

  public void testGetPlainUser() throws Exception {
    User user = authenticator.getUserForToken(USER_TOKEN);
    assertNotNull(user);
    assertEquals(user.getName(), USER_USERNAME);
    assertFalse(user.isAdmin());
    assertFalse(user.isDisabled());
  }

  public void testGetDisabledUser() throws Exception {
    User user = authenticator.getUserForToken(DISABLED_TOKEN);
    assertNotNull(user);
    assertEquals(user.getName(), DISABLED_USERNAME);
    assertFalse(user.isAdmin());
    assertTrue(user.isDisabled());
  }

}
