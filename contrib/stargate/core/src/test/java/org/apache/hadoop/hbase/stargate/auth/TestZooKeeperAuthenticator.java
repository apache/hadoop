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

import java.io.ByteArrayOutputStream;

import org.apache.hadoop.hbase.stargate.MiniClusterTestBase;
import org.apache.hadoop.hbase.stargate.User;
import org.apache.hadoop.hbase.stargate.auth.ZooKeeperAuthenticator.UserModel;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;

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
    super.setUp();
    authenticator = new ZooKeeperAuthenticator(conf);
    ZooKeeper zk = authenticator.wrapper.getZooKeeper();
    JSONJAXBContext context =
      new JSONJAXBContext(JSONConfiguration.natural().build(),
          UserModel.class);
    JSONMarshaller marshaller = context.createJSONMarshaller();
    if (zk.exists(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" + 
          ADMIN_TOKEN, null) == null) {
      UserModel model = new UserModel();
      model.name = ADMIN_USERNAME;
      model.admin = true;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      marshaller.marshallToJSON(model, os);
      zk.create(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" + ADMIN_TOKEN, 
        os.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" + 
          USER_TOKEN, null) == null) {
      UserModel model = new UserModel();
      model.name = USER_USERNAME;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      marshaller.marshallToJSON(model, os);
      zk.create(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" + USER_TOKEN, 
        os.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" + 
          DISABLED_TOKEN, null) == null) {
      UserModel model = new UserModel();
      model.name = DISABLED_USERNAME;
      model.disabled = true;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      marshaller.marshallToJSON(model, os);
      zk.create(ZooKeeperAuthenticator.USERS_ZNODE_ROOT + "/" +DISABLED_TOKEN,
        os.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
