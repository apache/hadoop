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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import junit.framework.TestCase;

public class TestHBCAuthenticator extends TestCase {

  static final String UNKNOWN_TOKEN = "00000000000000000000000000000000";
  static final String ADMIN_TOKEN = "e998efffc67c49c6e14921229a51b7b3";
  static final String ADMIN_USERNAME = "testAdmin";
  static final String USER_TOKEN = "da4829144e3a2febd909a6e1b4ed7cfa";
  static final String USER_USERNAME = "testUser";
  static final String DISABLED_TOKEN = "17de5b5db0fd3de0847bd95396f36d92";
  static final String DISABLED_USERNAME = "disabledUser";

  static Configuration conf;
  static HBCAuthenticator authenticator;
  static {
    conf = HBaseConfiguration.create();
    conf.set("stargate.auth.token." + USER_TOKEN, USER_USERNAME);
    conf.set("stargate.auth.user." + USER_USERNAME + ".admin", "false");
    conf.set("stargate.auth.user." + USER_USERNAME + ".disabled", "false");
    conf.set("stargate.auth.token." + ADMIN_TOKEN, ADMIN_USERNAME);
    conf.set("stargate.auth.user." + ADMIN_USERNAME + ".admin", "true");
    conf.set("stargate.auth.user." + ADMIN_USERNAME + ".disabled", "false");
    conf.set("stargate.auth.token." + DISABLED_TOKEN, DISABLED_USERNAME);
    conf.set("stargate.auth.user." + DISABLED_USERNAME + ".admin", "false");
    conf.set("stargate.auth.user." + DISABLED_USERNAME + ".disabled", "true");
    authenticator = new HBCAuthenticator(conf);
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
