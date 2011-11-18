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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class is for testing HCM features
 */
public class TestHCM {
  private static final Log LOG = LogFactory.getLog(TestHCM.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws InterruptedException 
   * @throws IllegalAccessException 
   * @throws NoSuchFieldException 
   * @throws ZooKeeperConnectionException 
   * @throws IllegalArgumentException 
   * @throws SecurityException 
   * @see https://issues.apache.org/jira/browse/HBASE-2925
   */
  // Disabling.  Of course this test will OOME using new Configuration each time
  // St.Ack 20110428
  // @Test
  public void testManyNewConnectionsDoesnotOOME()
  throws SecurityException, IllegalArgumentException,
  ZooKeeperConnectionException, NoSuchFieldException, IllegalAccessException,
  InterruptedException {
    createNewConfigurations();
  }

  private static Random _randy = new Random();

  public static void createNewConfigurations() throws SecurityException,
  IllegalArgumentException, NoSuchFieldException,
  IllegalAccessException, InterruptedException, ZooKeeperConnectionException {
    HConnection last = null;
    for (int i = 0; i <= (HConnectionManager.MAX_CACHED_HBASE_INSTANCES * 2); i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = HBaseConfiguration.create();
      configuration.set("somekey", String.valueOf(_randy.nextInt()));
      System.out.println("Hash Code: " + configuration.hashCode());
      HConnection connection = HConnectionManager.getConnection(configuration);
      if (last != null) {
        if (last == connection) {
          System.out.println("!! Got same connection for once !!");
        }
      }
      // change the configuration once, and the cached connection is lost forever:
      //      the hashtable holding the cache won't be able to find its own keys
      //      to remove them, so the LRU strategy does not work.
      configuration.set("someotherkey", String.valueOf(_randy.nextInt()));
      last = connection;
      LOG.info("Cache Size: " + getHConnectionManagerCacheSize());
      Thread.sleep(100);
    }
    Assert.assertEquals(1,
      getHConnectionManagerCacheSize());
  }

  private static int getHConnectionManagerCacheSize()
  throws SecurityException, NoSuchFieldException,
  IllegalArgumentException, IllegalAccessException {
    Field cacheField =
      HConnectionManager.class.getDeclaredField("HBASE_INSTANCES");
    cacheField.setAccessible(true);
    Map<?, ?> cache = (Map<?, ?>) cacheField.get(null);
    return cache.size();
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test
  public void testRegionCaching() throws Exception{
    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.HConnectionImplementation conn =
        (HConnectionManager.HConnectionImplementation)table.getConnection();
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    conn.deleteCachedLocation(TABLE_NAME, ROW);
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);
  }

  /**
   * Make sure that {@link HConfiguration} instances that are essentially the
   * same map to the same {@link HConnection} instance.
   */
  @Test
  public void testConnectionSameness() throws Exception {
    HConnection previousConnection = null;
    for (int i = 0; i < 2; i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = TEST_UTIL.getConfiguration();
      configuration.set("some_key", String.valueOf(_randy.nextInt()));
      LOG.info("The hash code of the current configuration is: "
          + configuration.hashCode());
      HConnection currentConnection = HConnectionManager
          .getConnection(configuration);
      if (previousConnection != null) {
        assertTrue(
            "Did not get the same connection even though its key didn't change",
            previousConnection == currentConnection);
      }
      previousConnection = currentConnection;
      // change the configuration, so that it is no longer reachable from the
      // client's perspective. However, since its part of the LRU doubly linked
      // list, it will eventually get thrown out, at which time it should also
      // close the corresponding {@link HConnection}.
      configuration.set("other_key", String.valueOf(_randy.nextInt()));
    }
  }

  /**
   * Makes sure that there is no leaking of
   * {@link HConnectionManager.TableServers} in the {@link HConnectionManager}
   * class.
   */
  @Test
  public void testConnectionUniqueness() throws Exception {
    HConnection previousConnection = null;
    for (int i = 0; i < HConnectionManager.MAX_CACHED_HBASE_INSTANCES + 10; i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = TEST_UTIL.getConfiguration();
      configuration.set("some_key", String.valueOf(_randy.nextInt()));
      configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
          String.valueOf(_randy.nextInt()));
      LOG.info("The hash code of the current configuration is: "
          + configuration.hashCode());
      HConnection currentConnection = HConnectionManager
          .getConnection(configuration);
      if (previousConnection != null) {
        assertTrue("Got the same connection even though its key changed!",
            previousConnection != currentConnection);
      }
      // change the configuration, so that it is no longer reachable from the
      // client's perspective. However, since its part of the LRU doubly linked
      // list, it will eventually get thrown out, at which time it should also
      // close the corresponding {@link HConnection}.
      configuration.set("other_key", String.valueOf(_randy.nextInt()));

      previousConnection = currentConnection;
      LOG.info("The current HConnectionManager#HBASE_INSTANCES cache size is: "
          + getHConnectionManagerCacheSize());
      Thread.sleep(50);
    }
  }

  @Test
  public void testClosing() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
        String.valueOf(_randy.nextInt()));

    HConnection c1 = HConnectionManager.createConnection(configuration);
    HConnection c2 = HConnectionManager.createConnection(configuration);

    HConnection c3 = HConnectionManager.getConnection(configuration);
    HConnection c4 = HConnectionManager.getConnection(configuration);
    assertTrue(c3 == c4);

    c1.close();
    assertTrue(c1.isClosed());
    assertFalse(c2.isClosed());
    assertFalse(c3.isClosed());

    c3.close();
    // still a reference left
    assertFalse(c3.isClosed());
    c3.close();
    assertTrue(c3.isClosed());
    // c3 was removed from the cache
    assertTrue(HConnectionManager.getConnection(configuration) != c3);

    assertFalse(c2.isClosed());
  }

  /**
   * Trivial test to verify that nobody messes with
   * {@link HConnectionManager#createConnection(Configuration)}
   */
  @Test
  public void testCreateConnection() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    HConnection c1 = HConnectionManager.createConnection(configuration);
    HConnection c2 = HConnectionManager.createConnection(configuration);
    // created from the same configuration, yet they are different
    assertTrue(c1 != c2);
    assertTrue(c1.getConfiguration() == c2.getConfiguration());
    // make sure these were not cached
    HConnection c3 = HConnectionManager.getConnection(configuration);
    assertTrue(c1 != c3);
    assertTrue(c2 != c3);
  }
}
