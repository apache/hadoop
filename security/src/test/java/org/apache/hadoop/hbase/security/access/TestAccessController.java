/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
public class TestAccessController {
  private static Log LOG = LogFactory.getLog(TestAccessController.class);
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user with all permissions
  private static User SUPERUSER;
  // table owner user
  private static User USER_OWNER;
  // user with rw permissions
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user with no permissions
  private static User USER_NONE;

  private static byte[] TEST_TABLE = Bytes.toBytes("testtable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster()
        .getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController)cpHost.findCoprocessor(
        AccessController.class.getName());
    CP_ENV = cpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
        Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[]{"supergroup"});
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    htd.setOwnerString(USER_OWNER.getShortName());
    admin.createTable(htd);

    // initilize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, TEST_TABLE);
    protocol.grant(Bytes.toBytes(USER_RW.getShortName()),
        new TablePermission(TEST_TABLE, TEST_FAMILY, Permission.Action.READ,
            Permission.Action.WRITE));

    protocol.grant(Bytes.toBytes(USER_RO.getShortName()),
        new TablePermission(TEST_TABLE, TEST_FAMILY, Permission.Action.READ));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction action)
    throws Exception {
    try {
      user.runAs(action);
    } catch (AccessDeniedException ade) {
      fail("Expected action to pass for user '" + user.getShortName() +
          "' but was denied");
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction action)
    throws Exception {
    try {
      user.runAs(action);
      fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
    } catch (RetriesExhaustedWithDetailsException e) {
      // in case of batch operations, and put, the client assembles a
      // RetriesExhaustedWithDetailsException instead of throwing an
      // AccessDeniedException
      boolean isAccessDeniedException = false;
      for ( Throwable ex : e.getCauses()) {
        if (ex instanceof AccessDeniedException) {
          isAccessDeniedException = true;
          break;
        }
      }
      if (!isAccessDeniedException ) {
        fail("Not receiving AccessDeniedException for user '" +
            user.getShortName() + "'");
      }
    } catch (AccessDeniedException ade) {
      // expected result
    }
  }

  @Test
  public void testTableCreate() throws Exception {
    PrivilegedExceptionAction createTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor("testnewtable");
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(
            ObserverContext.createAndPrepare(CP_ENV, null), htd, null);
        return null;
      }
    };

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, createTable);

    // all others should be denied
    verifyDenied(USER_OWNER, createTable);
    verifyDenied(USER_RW, createTable);
    verifyDenied(USER_RO, createTable);
    verifyDenied(USER_NONE, createTable);
  }

  @Test
  public void testTableModify() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor("fam_"+User.getCurrent().getShortName()));
        ACCESS_CONTROLLER.preModifyTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE, htd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testTableDelete() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testAddColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor("fam_new");
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAddColumn(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE, hcd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testModifyColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(10);
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyColumn(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE, hcd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testDeleteColumn() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteColumn(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE, TEST_FAMILY);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testTableDisable() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testTableEnable() throws Exception {
    PrivilegedExceptionAction enableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preEnableTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, enableTable);
    verifyDenied(USER_RW, enableTable);
    verifyDenied(USER_RO, enableTable);
    verifyDenied(USER_NONE, enableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, enableTable);
  }

  @Test
  public void testMove() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();
    final ServerName server = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preMove(ObserverContext.createAndPrepare(CP_ENV, null),
            firstRegion.getKey(), server, server);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testAssign() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAssign(ObserverContext.createAndPrepare(CP_ENV, null),
            firstRegion.getKey());
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testUnassign() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUnassign(ObserverContext.createAndPrepare(CP_ENV, null),
            firstRegion.getKey(), false);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testBalance() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testBalanceSwitch() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalanceSwitch(ObserverContext.createAndPrepare(CP_ENV, null), true);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testShutdown() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preShutdown(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testStopMaster() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopMaster(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  private void verifyWrite(PrivilegedExceptionAction action) throws Exception {
    // should be denied
    verifyDenied(USER_NONE, action);
    verifyDenied(USER_RO, action);

    // should be allowed
    verifyAllowed(SUPERUSER, action);
    verifyAllowed(USER_OWNER, action);
    verifyAllowed(USER_RW, action);
  }

  private void verifyRead(PrivilegedExceptionAction action) throws Exception {
    // should be denied
    verifyDenied(USER_NONE, action);

    // should be allowed
    verifyAllowed(SUPERUSER, action);
    verifyAllowed(USER_OWNER, action);
    verifyAllowed(USER_RW, action);
    verifyAllowed(USER_RO, action);
  }

  @Test
  public void testRead() throws Exception {
    // get action
    PrivilegedExceptionAction getAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(TEST_FAMILY);
        HTable t = new HTable(conf, TEST_TABLE);
        t.get(g);
        return null;
      }
    };
    verifyRead(getAction);

    // action for scanning
    PrivilegedExceptionAction scanAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Scan s = new Scan();
        s.addFamily(TEST_FAMILY);

        HTable table = new HTable(conf, TEST_TABLE);
        ResultScanner scanner = table.getScanner(s);
        try {
          for (Result r = scanner.next(); r != null; r = scanner.next()) {
            // do nothing
          }
        } catch (IOException e) {
        } finally {
          scanner.close();
        }
        return null;
      }
    };
    verifyRead(scanAction);
  }

  @Test
  // test put, delete, increment
  public void testWrite() throws Exception {
    // put action
    PrivilegedExceptionAction putAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("random_row"));
        p.add(TEST_FAMILY, Bytes.toBytes("Qualifier"), Bytes.toBytes(1));
        HTable t = new HTable(conf, TEST_TABLE);
        t.put(p);
        return null;
      }
    };
    verifyWrite(putAction);

    // delete action
    PrivilegedExceptionAction deleteAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(TEST_FAMILY);
        HTable t = new HTable(conf, TEST_TABLE);
        t.delete(d);
        return null;
      }
    };
    verifyWrite(deleteAction);

    // increment action
    PrivilegedExceptionAction incrementAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Increment inc = new Increment(Bytes.toBytes("random_row"));
        inc.addColumn(TEST_FAMILY, Bytes.toBytes("Qualifier"), 1);
        HTable t = new HTable(conf, TEST_TABLE);
        t.increment(inc);
        return null;
      }
    };
    verifyWrite(incrementAction);
  }

  @Test
  public void testGrantRevoke() throws Exception {
    final byte[] tableName = Bytes.toBytes("TempTable");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwnerString(USER_OWNER.getShortName());
    admin.createTable(htd);

    // create temp users
    User user = User.createUserForTesting(TEST_UTIL.getConfiguration(),
        "user", new String[0]);

    // perms only stored against the first region
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        acl.coprocessorProxy(AccessControllerProtocol.class,
            tableName);

    // prepare actions:
    PrivilegedExceptionAction putActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        p.add(family2, qualifier, Bytes.toBytes("v2"));
        HTable t = new HTable(conf, tableName);
        t.put(p);
        return null;
      }
    };
    PrivilegedExceptionAction putAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        HTable t = new HTable(conf, tableName);
        t.put(p);
        return null;
      }
    };
    PrivilegedExceptionAction putAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family2, qualifier, Bytes.toBytes("v2"));
        HTable t = new HTable(conf, tableName);
        t.put(p);
        return null;
      }
    };
    PrivilegedExceptionAction getActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family1);
        g.addFamily(family2);
        HTable t = new HTable(conf, tableName);
        t.get(g);
        return null;
      }
    };
    PrivilegedExceptionAction getAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family1);
        HTable t = new HTable(conf, tableName);
        t.get(g);
        return null;
      }
    };
    PrivilegedExceptionAction getAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family2);
        HTable t = new HTable(conf, tableName);
        t.get(g);
        return null;
      }
    };
    PrivilegedExceptionAction deleteActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family1);
        d.deleteFamily(family2);
        HTable t = new HTable(conf, tableName);
        t.delete(d);
        return null;
      }
    };
    PrivilegedExceptionAction deleteAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family1);
        HTable t = new HTable(conf, tableName);
        t.delete(d);
        return null;
      }
    };
    PrivilegedExceptionAction deleteAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family2);
        HTable t = new HTable(conf, tableName);
        t.delete(d);
        return null;
      }
    };

    // initial check:
    verifyDenied(user, getActionAll);
    verifyDenied(user, getAction1);
    verifyDenied(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyDenied(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyDenied(user, deleteAction2);

    // grant table read permission
    protocol.grant(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, null, Permission.Action.READ));
    Thread.sleep(100);
    // check
    verifyAllowed(user, getActionAll);
    verifyAllowed(user, getAction1);
    verifyAllowed(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyDenied(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyDenied(user, deleteAction2);

    // grant table write permission
    protocol.grant(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, null, Permission.Action.WRITE));
    Thread.sleep(100);
    verifyDenied(user, getActionAll);
    verifyDenied(user, getAction1);
    verifyDenied(user, getAction2);

    verifyAllowed(user, putActionAll);
    verifyAllowed(user, putAction1);
    verifyAllowed(user, putAction2);

    verifyAllowed(user, deleteActionAll);
    verifyAllowed(user, deleteAction1);
    verifyAllowed(user, deleteAction2);

    // revoke table permission
    protocol.grant(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, null, Permission.Action.READ,
        Permission.Action.WRITE));

    protocol.revoke(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, null));
    Thread.sleep(100);
    verifyDenied(user, getActionAll);
    verifyDenied(user, getAction1);
    verifyDenied(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyDenied(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyDenied(user, deleteAction2);

    // grant column family read permission
    protocol.grant(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, family1, Permission.Action.READ));
    Thread.sleep(100);

    verifyAllowed(user, getActionAll);
    verifyAllowed(user, getAction1);
    verifyDenied(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyDenied(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyDenied(user, deleteAction2);

    // grant column family write permission
    protocol.grant(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, family2, Permission.Action.WRITE));
    Thread.sleep(100);

    verifyAllowed(user, getActionAll);
    verifyAllowed(user, getAction1);
    verifyDenied(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyAllowed(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyAllowed(user, deleteAction2);

    // revoke column family permission
    protocol.revoke(Bytes.toBytes(user.getShortName()),
      new TablePermission(tableName, family2));
    Thread.sleep(100);

    verifyAllowed(user, getActionAll);
    verifyAllowed(user, getAction1);
    verifyDenied(user, getAction2);

    verifyDenied(user, putActionAll);
    verifyDenied(user, putAction1);
    verifyDenied(user, putAction2);

    verifyDenied(user, deleteActionAll);
    verifyDenied(user, deleteAction1);
    verifyDenied(user, deleteAction2);

    // delete table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  private boolean hasFoundUserPermission(UserPermission userPermission,
                                         List<UserPermission> perms) {
    return perms.contains(userPermission);
  }

  @Test
  public void testGrantRevokeAtQualifierLevel() throws Exception {
    final byte[] tableName = Bytes.toBytes("testGrantRevokeAtQualifierLevel");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();

    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwnerString(USER_OWNER.getShortName());
    admin.createTable(htd);

    // create temp users
    User user = User.createUserForTesting(TEST_UTIL.getConfiguration(),
        "user", new String[0]);

    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        acl.coprocessorProxy(AccessControllerProtocol.class, tableName);

    PrivilegedExceptionAction getQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addColumn(family1, qualifier);
        HTable t = new HTable(conf, tableName);
        t.get(g);
        return null;
      }
    };
    PrivilegedExceptionAction putQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("random_row"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        HTable t = new HTable(conf, tableName);
        t.put(p);
        return null;
      }
    };
    PrivilegedExceptionAction deleteQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteColumn(family1, qualifier);
        //d.deleteFamily(family1);
        HTable t = new HTable(conf, tableName);
        t.delete(d);
        return null;
      }
    };

    protocol.revoke(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, family1));
    verifyDenied(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    protocol.grant(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, family1, qualifier,
            Permission.Action.READ));
    Thread.sleep(100);

    verifyAllowed(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    // only grant write permission
    // TODO: comment this portion after HBASE-3583
    protocol.grant(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, family1, qualifier,
            Permission.Action.WRITE));
    Thread.sleep(100);

    verifyDenied(user, getQualifierAction);
    verifyAllowed(user, putQualifierAction);
    verifyAllowed(user, deleteQualifierAction);

    // grant both read and write permission.
    protocol.grant(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, family1, qualifier,
            Permission.Action.READ, Permission.Action.WRITE));
    Thread.sleep(100);

    verifyAllowed(user, getQualifierAction);
    verifyAllowed(user, putQualifierAction);
    verifyAllowed(user, deleteQualifierAction);

    // revoke family level permission won't impact column level.
    protocol.revoke(Bytes.toBytes(user.getShortName()),
        new TablePermission(tableName, family1, qualifier));
    Thread.sleep(100);

    verifyDenied(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    // delete table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testPermissionList() throws Exception {
    final byte[] tableName = Bytes.toBytes("testPermissionList");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");
    final byte[] user = Bytes.toBytes("user");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwnerString(USER_OWNER.getShortName());
    admin.createTable(htd);

    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        acl.coprocessorProxy(AccessControllerProtocol.class, tableName);

    List<UserPermission> perms = protocol.getUserPermissions(tableName);

    UserPermission up = new UserPermission(user,
        tableName, family1, qualifier, Permission.Action.READ);
    assertFalse("User should not be granted permission: " + up.toString(),
        hasFoundUserPermission(up, perms));

    // grant read permission
    UserPermission upToSet = new UserPermission(user,
        tableName, family1, qualifier, Permission.Action.READ);
    protocol.grant(user, upToSet);
    perms = protocol.getUserPermissions(tableName);

    UserPermission upToVerify = new UserPermission(user,
        tableName, family1, qualifier, Permission.Action.READ);
    assertTrue("User should be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

    upToVerify = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.WRITE);
    assertFalse("User should not be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

    // grant read+write
    upToSet = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);
    protocol.grant(user, upToSet);
    perms = protocol.getUserPermissions(tableName);

    upToVerify = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);
    assertTrue("User should be granted permission: " + upToVerify.toString(),
            hasFoundUserPermission(upToVerify, perms));

    protocol.revoke(user, upToSet);
    perms = protocol.getUserPermissions(tableName);
    assertFalse("User should not be granted permission: " + upToVerify.toString(),
      hasFoundUserPermission(upToVerify, perms));

    // delete table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
