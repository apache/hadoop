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

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import junit.framework.TestCase;

public class TestJDBCAuthenticator extends TestCase {

  static final Log LOG = LogFactory.getLog(TestJDBCAuthenticator.class);

  static final String TABLE = "users";
  static final String JDBC_URL = "jdbc:hsqldb:mem:test";

  static final String UNKNOWN_TOKEN = "00000000000000000000000000000000";
  static final String ADMIN_TOKEN = "e998efffc67c49c6e14921229a51b7b3";
  static final String ADMIN_USERNAME = "testAdmin";
  static final String USER_TOKEN = "da4829144e3a2febd909a6e1b4ed7cfa";
  static final String USER_USERNAME = "testUser";
  static final String DISABLED_TOKEN = "17de5b5db0fd3de0847bd95396f36d92";
  static final String DISABLED_USERNAME = "disabledUser";

  static JDBCAuthenticator authenticator;
  static {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
      Connection c = DriverManager.getConnection(JDBC_URL, "SA", "");
      c.createStatement().execute(
        "CREATE TABLE " + TABLE + " ( " +
          "token CHAR(32) PRIMARY KEY, " +
          "name VARCHAR(32), " +
          "admin BOOLEAN, " +
          "disabled BOOLEAN " +
        ")");
      c.createStatement().execute(
        "INSERT INTO " + TABLE + " ( token,name,admin,disabled ) " +
        "VALUES ( '" + ADMIN_TOKEN + "','" + ADMIN_USERNAME +
          "',TRUE,FALSE )");
      c.createStatement().execute(
        "INSERT INTO " + TABLE + " ( token,name,admin,disabled ) " +
        "VALUES ( '" + USER_TOKEN + "','" + USER_USERNAME +
          "',FALSE,FALSE )");
      c.createStatement().execute(
        "INSERT INTO " + TABLE + " ( token,name,admin,disabled ) " +
        "VALUES ( '" + DISABLED_TOKEN + "','" + DISABLED_USERNAME +
          "',FALSE,TRUE )");
      c.createStatement().execute("CREATE USER test PASSWORD access");
      c.createStatement().execute("GRANT ALL ON " + TABLE + " TO test");
      c.close();
      authenticator = new JDBCAuthenticator(JDBC_URL, TABLE, "test",
        "access");
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
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
