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
package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Test the Mount Table entry in the State Store.
 */
public class TestMountTable {

  private static final String SRC = "/test";
  private static final String DST_NS_0 = "ns0";
  private static final String DST_NS_1 = "ns1";
  private static final String DST_PATH_0 = "/path1";
  private static final String DST_PATH_1 = "/path/path2";
  private static final List<RemoteLocation> DST = new LinkedList<>();
  static {
    DST.add(new RemoteLocation(DST_NS_0, DST_PATH_0, SRC));
    DST.add(new RemoteLocation(DST_NS_1, DST_PATH_1, SRC));
  }
  private static final Map<String, String> DST_MAP = new LinkedHashMap<>();
  static {
    DST_MAP.put(DST_NS_0, DST_PATH_0);
    DST_MAP.put(DST_NS_1, DST_PATH_1);
  }

  private static final long DATE_CREATED = 100;
  private static final long DATE_MOD = 200;

  private static final long NS_COUNT = 1;
  private static final long NS_QUOTA = 5;
  private static final long SS_COUNT = 10;
  private static final long SS_QUOTA = 100;

  private static final RouterQuotaUsage QUOTA = new RouterQuotaUsage.Builder()
      .fileAndDirectoryCount(NS_COUNT).quota(NS_QUOTA).spaceConsumed(SS_COUNT)
      .spaceQuota(SS_QUOTA).build();

  @Test
  public void testGetterSetter() throws IOException {

    MountTable record = MountTable.newInstance(SRC, DST_MAP);

    validateDestinations(record);
    assertEquals(SRC, record.getSourcePath());
    assertEquals(DST, record.getDestinations());
    assertTrue(DATE_CREATED > 0);
    assertTrue(DATE_MOD > 0);

    RouterQuotaUsage quota = record.getQuota();
    assertEquals(0, quota.getFileAndDirectoryCount());
    assertEquals(HdfsConstants.QUOTA_RESET, quota.getQuota());
    assertEquals(0, quota.getSpaceConsumed());
    assertEquals(HdfsConstants.QUOTA_RESET, quota.getSpaceQuota());

    MountTable record2 =
        MountTable.newInstance(SRC, DST_MAP, DATE_CREATED, DATE_MOD);

    validateDestinations(record2);
    assertEquals(SRC, record2.getSourcePath());
    assertEquals(DST, record2.getDestinations());
    assertEquals(DATE_CREATED, record2.getDateCreated());
    assertEquals(DATE_MOD, record2.getDateModified());
    assertFalse(record.isReadOnly());
    assertEquals(DestinationOrder.HASH, record.getDestOrder());
  }

  @Test
  public void testSerialization() throws IOException {
    testSerialization(DestinationOrder.RANDOM);
    testSerialization(DestinationOrder.HASH);
    testSerialization(DestinationOrder.LOCAL);
  }

  private void testSerialization(final DestinationOrder order)
      throws IOException {

    MountTable record = MountTable.newInstance(
        SRC, DST_MAP, DATE_CREATED, DATE_MOD);
    record.setReadOnly(true);
    record.setDestOrder(order);
    record.setQuota(QUOTA);

    StateStoreSerializer serializer = StateStoreSerializer.getSerializer();
    String serializedString = serializer.serializeString(record);
    MountTable record2 =
        serializer.deserialize(serializedString, MountTable.class);

    validateDestinations(record2);
    assertEquals(SRC, record2.getSourcePath());
    assertEquals(DST, record2.getDestinations());
    assertEquals(DATE_CREATED, record2.getDateCreated());
    assertEquals(DATE_MOD, record2.getDateModified());
    assertTrue(record2.isReadOnly());
    assertEquals(order, record2.getDestOrder());

    RouterQuotaUsage quotaGet = record2.getQuota();
    assertEquals(NS_COUNT, quotaGet.getFileAndDirectoryCount());
    assertEquals(NS_QUOTA, quotaGet.getQuota());
    assertEquals(SS_COUNT, quotaGet.getSpaceConsumed());
    assertEquals(SS_QUOTA, quotaGet.getSpaceQuota());
  }

  @Test
  public void testReadOnly() throws IOException {

    Map<String, String> dest = new LinkedHashMap<>();
    dest.put(DST_NS_0, DST_PATH_0);
    dest.put(DST_NS_1, DST_PATH_1);
    MountTable record1 = MountTable.newInstance(SRC, dest);
    record1.setReadOnly(true);

    validateDestinations(record1);
    assertEquals(SRC, record1.getSourcePath());
    assertEquals(DST, record1.getDestinations());
    assertTrue(DATE_CREATED > 0);
    assertTrue(DATE_MOD > 0);
    assertTrue(record1.isReadOnly());

    MountTable record2 = MountTable.newInstance(
        SRC, DST_MAP, DATE_CREATED, DATE_MOD);
    record2.setReadOnly(true);

    validateDestinations(record2);
    assertEquals(SRC, record2.getSourcePath());
    assertEquals(DST, record2.getDestinations());
    assertEquals(DATE_CREATED, record2.getDateCreated());
    assertEquals(DATE_MOD, record2.getDateModified());
    assertTrue(record2.isReadOnly());
  }

  @Test
  public void testFaultTolerant() throws IOException {

    Map<String, String> dest = new LinkedHashMap<>();
    dest.put(DST_NS_0, DST_PATH_0);
    dest.put(DST_NS_1, DST_PATH_1);
    MountTable record0 = MountTable.newInstance(SRC, dest);
    assertFalse(record0.isFaultTolerant());

    MountTable record1 = MountTable.newInstance(SRC, dest);
    assertFalse(record1.isFaultTolerant());
    assertEquals(record0, record1);

    record1.setFaultTolerant(true);
    assertTrue(record1.isFaultTolerant());
    assertNotEquals(record0, record1);
  }

  @Test
  public void testOrder() throws IOException {
    testOrder(DestinationOrder.HASH);
    testOrder(DestinationOrder.LOCAL);
    testOrder(DestinationOrder.RANDOM);
  }

  private void testOrder(final DestinationOrder order)
      throws IOException {

    MountTable record = MountTable.newInstance(
        SRC, DST_MAP, DATE_CREATED, DATE_MOD);
    record.setDestOrder(order);

    validateDestinations(record);
    assertEquals(SRC, record.getSourcePath());
    assertEquals(DST, record.getDestinations());
    assertEquals(DATE_CREATED, record.getDateCreated());
    assertEquals(DATE_MOD, record.getDateModified());
    assertEquals(order, record.getDestOrder());
  }

  private void validateDestinations(MountTable record) {

    assertEquals(SRC, record.getSourcePath());
    assertEquals(2, record.getDestinations().size());

    RemoteLocation location1 = record.getDestinations().get(0);
    assertEquals(DST_NS_0, location1.getNameserviceId());
    assertEquals(DST_PATH_0, location1.getDest());

    RemoteLocation location2 = record.getDestinations().get(1);
    assertEquals(DST_NS_1, location2.getNameserviceId());
    assertEquals(DST_PATH_1, location2.getDest());
  }

  @Test
  public void testQuota() throws IOException {
    MountTable record = MountTable.newInstance(SRC, DST_MAP);
    record.setQuota(QUOTA);

    validateDestinations(record);
    assertEquals(SRC, record.getSourcePath());
    assertEquals(DST, record.getDestinations());
    assertTrue(DATE_CREATED > 0);
    assertTrue(DATE_MOD > 0);

    RouterQuotaUsage quotaGet = record.getQuota();
    assertEquals(NS_COUNT, quotaGet.getFileAndDirectoryCount());
    assertEquals(NS_QUOTA, quotaGet.getQuota());
    assertEquals(SS_COUNT, quotaGet.getSpaceConsumed());
    assertEquals(SS_QUOTA, quotaGet.getSpaceQuota());
  }

  @Test
  public void testValidation() throws IOException {
    Map<String, String> destinations = new HashMap<>();
    destinations.put("ns0", "/testValidate-dest");
    try {
      MountTable.newInstance("testValidate", destinations);
      fail("Mount table entry should be created failed.");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          MountTable.ERROR_MSG_MUST_START_WITH_BACK_SLASH, e);
    }

    destinations.clear();
    destinations.put("ns0", "testValidate-dest");
    try {
      MountTable.newInstance("/testValidate", destinations);
      fail("Mount table entry should be created failed.");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          MountTable.ERROR_MSG_ALL_DEST_MUST_START_WITH_BACK_SLASH, e);
    }

    destinations.clear();
    destinations.put("", "/testValidate-dest");
    try {
      MountTable.newInstance("/testValidate", destinations);
      fail("Mount table entry should be created failed.");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          MountTable.ERROR_MSG_INVAILD_DEST_NS, e);
    }

    destinations.clear();
    destinations.put("ns0", "/testValidate-dest");
    MountTable record = MountTable.newInstance("/testValidate", destinations);
    assertNotNull(record);

  }
}