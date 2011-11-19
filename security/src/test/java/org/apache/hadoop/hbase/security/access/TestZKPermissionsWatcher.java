/*
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

package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test the reading and writing of access permissions to and from zookeeper.
 */
public class TestZKPermissionsWatcher {
  private static final Log LOG = LogFactory.getLog(TestZKPermissionsWatcher.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static TableAuthManager AUTH_A;
  private static TableAuthManager AUTH_B;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  private static byte[] TEST_TABLE = Bytes.toBytes("perms_test");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    // start minicluster
    UTIL.startMiniCluster();
    AUTH_A = TableAuthManager.get(new ZooKeeperWatcher(conf,
      "TestZKPermissionsWatcher_1", ABORTABLE), conf);
    AUTH_B = TableAuthManager.get(new ZooKeeperWatcher(conf,
      "TestZKPermissionsWatcher_2", ABORTABLE), conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPermissionsWatcher() throws Exception {
    assertFalse(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertFalse(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));

    assertFalse(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertFalse(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));

    // update ACL: george RW
    List<TablePermission> acl = new ArrayList<TablePermission>();
    acl.add(new TablePermission(TEST_TABLE, null, TablePermission.Action.READ,
      TablePermission.Action.WRITE));
    AUTH_A.setUserPermissions("george", TEST_TABLE, acl);
    Thread.sleep(100);

    // check it
    assertTrue(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertTrue(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertTrue(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertFalse(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertFalse(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));

    // update ACL: hubert R
    acl = new ArrayList<TablePermission>();
    acl.add(new TablePermission(TEST_TABLE, null, TablePermission.Action.READ));
    AUTH_B.setUserPermissions("hubert", TEST_TABLE, acl);
    Thread.sleep(100);

    // check it
    assertTrue(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertTrue(AUTH_A.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertTrue(AUTH_B.authorizeUser("george", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertTrue(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_A.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.READ));
    assertFalse(AUTH_B.authorizeUser("hubert", TEST_TABLE, null,
      TablePermission.Action.WRITE));
  }
}
