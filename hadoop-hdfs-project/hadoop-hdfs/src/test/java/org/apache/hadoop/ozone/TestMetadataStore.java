/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.junit.Rule;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Collection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.runners.Parameterized.*;

/**
 * Test class for ozone metadata store.
 */
@RunWith(Parameterized.class)
public class TestMetadataStore {

  private final String storeImpl;

  public TestMetadataStore(String metadataImpl) {
    this.storeImpl = metadataImpl;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB},
        {OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB}
    });
  }

  private MetadataStore store;
  private File testDir;
  private final static int MAX_GETRANGE_LENGTH = 100;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void init() throws IOException {
    testDir = GenericTestUtils.getTestDir(getClass().getSimpleName()
        + "-" + storeImpl.toLowerCase());

    Configuration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, storeImpl);

    store = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setCreateIfMissing(true)
        .setDbFile(testDir)
        .build();

    // Add 20 entries.
    // {a0 : a-value0} to {a9 : a-value9}
    // {b0 : a-value0} to {b0 : b-value9}
    for (int i=0; i<10; i++) {
      store.put(getBytes("a" + i), getBytes("a-value" + i));
      store.put(getBytes("b" + i), getBytes("b-value" + i));
    }
  }

  @After
  public void cleanup() throws IOException {
    store.close();
    store.destroy();
    FileUtils.deleteDirectory(testDir);
  }

  private byte[] getBytes(String str) {
    return str == null ? null :
        DFSUtilClient.string2Bytes(str);
  }

  private String getString(byte[] bytes) {
    return bytes == null ? null :
        DFSUtilClient.bytes2String(bytes);
  }

  @Test
  public void testGetDelete() throws IOException {
    for (int i=0; i<10; i++) {
      byte[] va = store.get(getBytes("a" + i));
      Assert.assertEquals("a-value" + i, getString(va));

      byte[] vb = store.get(getBytes("b" + i));
      Assert.assertEquals("b-value" + i, getString(vb));
    }

    String keyToDel = "del-" + UUID.randomUUID().toString();
    store.put(getBytes(keyToDel), getBytes(keyToDel));
    Assert.assertEquals(keyToDel, getString(store.get(getBytes(keyToDel))));
    store.delete(getBytes(keyToDel));
    Assert.assertEquals(null, store.get(getBytes(keyToDel)));
  }

  @Test
  public void testPeekFrom() throws IOException {
    // Test peek from an element that has prev as well as next
    testPeek("a3", "a2", "a4");

    // Test peek from an element that only has prev
    testPeek("b9", "b8", null);

    // Test peek from an element that only has next
    testPeek("a0", null, "a1");
  }

  private String getExpectedValue(String key) {
    if (key == null) {
      return null;
    }
    char[] arr = key.toCharArray();
    return new StringBuffer().append(arr[0]).append("-value")
        .append(arr[arr.length - 1]).toString();
  }

  private void testPeek(String peekKey, String prevKey, String nextKey)
      throws IOException {
    // Look for current
    String k = null;
    String v = null;
    ImmutablePair<byte[], byte[]> current =
        store.peekAround(0, getBytes(peekKey));
    if (current != null) {
      k = getString(current.getKey());
      v = getString(current.getValue());
    }
    Assert.assertEquals(peekKey, k);
    Assert.assertEquals(v, getExpectedValue(peekKey));

    // Look for prev
    k = null;
    v = null;
    ImmutablePair<byte[], byte[]> prev =
        store.peekAround(-1, getBytes(peekKey));
    if (prev != null) {
      k = getString(prev.getKey());
      v = getString(prev.getValue());
    }
    Assert.assertEquals(prevKey, k);
    Assert.assertEquals(v, getExpectedValue(prevKey));

    // Look for next
    k = null;
    v = null;
    ImmutablePair<byte[], byte[]> next =
        store.peekAround(1, getBytes(peekKey));
    if (next != null) {
      k = getString(next.getKey());
      v = getString(next.getValue());
    }
    Assert.assertEquals(nextKey, k);
    Assert.assertEquals(v, getExpectedValue(nextKey));
  }

  @Test
  public void testIterateKeys() throws IOException {
    // iterate keys from b0
    ArrayList<String> result = Lists.newArrayList();
    store.iterate(getBytes("b0"), (k, v) -> {
      // b-value{i}
      String value = getString(v);
      char num = value.charAt(value.length() - 1);
      // each value adds 1
      int i = Character.getNumericValue(num) + 1;
      value =  value.substring(0, value.length() - 1) + i;
      result.add(value);
      return true;
    });

    Assert.assertFalse(result.isEmpty());
    for (int i=0; i<result.size(); i++) {
      Assert.assertEquals("b-value" + (i+1), result.get(i));
    }

    // iterate from a non exist key
    result.clear();
    store.iterate(getBytes("xyz"), (k, v) -> {
      result.add(getString(v));
      return true;
    });
    Assert.assertTrue(result.isEmpty());

    // iterate from the beginning
    result.clear();
    store.iterate(null, (k, v) -> {
      result.add(getString(v));
      return true;
    });
    Assert.assertEquals(20, result.size());
  }

  @Test
  public void testGetRangeKVs() throws IOException {
    List<Map.Entry<byte[], byte[]>> result = null;

    // Set empty startKey will return values from beginning.
    result = store.getRangeKVs(null, 5);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals("a-value2", getString(result.get(2).getValue()));

    // Returns max available entries after a valid startKey.
    result = store.getRangeKVs(getBytes("b0"), MAX_GETRANGE_LENGTH);
    Assert.assertEquals(10, result.size());
    Assert.assertEquals("b0", getString(result.get(0).getKey()));
    Assert.assertEquals("b-value0", getString(result.get(0).getValue()));
    result = store.getRangeKVs(getBytes("b0"), 5);
    Assert.assertEquals(5, result.size());

    // Both startKey and count are honored.
    result = store.getRangeKVs(getBytes("a9"), 2);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("a9", getString(result.get(0).getKey()));
    Assert.assertEquals("a-value9", getString(result.get(0).getValue()));
    Assert.assertEquals("b0", getString(result.get(1).getKey()));
    Assert.assertEquals("b-value0", getString(result.get(1).getValue()));

    // Filter keys by prefix.
    // It should returns all "b*" entries.
    MetadataKeyFilter filter1 = new KeyPrefixFilter("b");
    result = store.getRangeKVs(null, 100, filter1);
    Assert.assertEquals(10, result.size());
    Assert.assertTrue(result.stream().allMatch(entry ->
        new String(entry.getKey()).startsWith("b")
    ));
    Assert.assertEquals(20, filter1.getKeysScannedNum());
    Assert.assertEquals(10, filter1.getKeysHintedNum());
    result = store.getRangeKVs(null, 3, filter1);
    Assert.assertEquals(3, result.size());
    result = store.getRangeKVs(getBytes("b3"), 1, filter1);
    Assert.assertEquals("b-value3", getString(result.get(0).getValue()));

    // Define a customized filter that filters keys by suffix.
    // Returns all "*2" entries.
    MetadataKeyFilter filter2 = (preKey, currentKey, nextKey)
        -> getString(currentKey).endsWith("2");
    result = store.getRangeKVs(null, MAX_GETRANGE_LENGTH, filter2);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("a2", getString(result.get(0).getKey()));
    Assert.assertEquals("b2", getString(result.get(1).getKey()));
    result = store.getRangeKVs(null, 1, filter2);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("a2", getString(result.get(0).getKey()));

    // Apply multiple filters.
    result = store.getRangeKVs(null, MAX_GETRANGE_LENGTH, filter1, filter2);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("b2", getString(result.get(0).getKey()));
    Assert.assertEquals("b-value2", getString(result.get(0).getValue()));

    // If filter is null, no effect.
    result = store.getRangeKVs(null, 1, null);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("a0", getString(result.get(0).getKey()));
  }

  @Test
  public void testGetSequentialRangeKVs() throws IOException {
    MetadataKeyFilter suffixFilter = (preKey, currentKey, nextKey)
        -> DFSUtil.bytes2String(currentKey).endsWith("2");
    // Suppose to return a2 and b2
    List<Map.Entry<byte[], byte[]>> result =
        store.getRangeKVs(null, MAX_GETRANGE_LENGTH, suffixFilter);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("a2", DFSUtil.bytes2String(result.get(0).getKey()));
    Assert.assertEquals("b2", DFSUtil.bytes2String(result.get(1).getKey()));

    // Suppose to return just a2, because when it iterates to a3,
    // the filter no long matches and it should stop from there.
    result = store.getSequentialRangeKVs(null,
        MAX_GETRANGE_LENGTH, suffixFilter);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("a2", DFSUtil.bytes2String(result.get(0).getKey()));
  }

  @Test
  public void testGetRangeLength() throws IOException {
    List<Map.Entry<byte[], byte[]>> result = null;

    result = store.getRangeKVs(null, 0);
    Assert.assertEquals(0, result.size());

    result = store.getRangeKVs(null, 1);
    Assert.assertEquals(1, result.size());

    // Count less than zero is invalid.
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid count given");
    store.getRangeKVs(null, -1);
  }

  @Test
  public void testInvalidStartKey() throws IOException {
    // If startKey is invalid, the returned list should be empty.
    List<Map.Entry<byte[], byte[]>> kvs =
        store.getRangeKVs(getBytes("unknownKey"), MAX_GETRANGE_LENGTH);
    Assert.assertEquals(kvs.size(), 0);
  }

  @Test
  public void testDestroyDB() throws IOException {
    // create a new DB to test db destroy
    Configuration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, storeImpl);

    File dbDir = GenericTestUtils.getTestDir(getClass().getSimpleName()
        + "-" + storeImpl.toLowerCase() + "-toDestroy");
    MetadataStore dbStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setCreateIfMissing(true)
        .setDbFile(dbDir)
        .build();

    dbStore.put(getBytes("key1"), getBytes("value1"));
    dbStore.put(getBytes("key2"), getBytes("value2"));

    Assert.assertFalse(dbStore.isEmpty());
    Assert.assertTrue(dbDir.exists());
    Assert.assertTrue(dbDir.listFiles().length > 0);

    dbStore.destroy();

    Assert.assertFalse(dbDir.exists());
  }

  @Test
  public void testBatchWrite() throws IOException {
    Configuration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL, storeImpl);

    File dbDir = GenericTestUtils.getTestDir(getClass().getSimpleName()
        + "-" + storeImpl.toLowerCase() + "-batchWrite");
    MetadataStore dbStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setCreateIfMissing(true)
        .setDbFile(dbDir)
        .build();

    List<String> expectedResult = Lists.newArrayList();
    for (int i = 0; i<10; i++) {
      dbStore.put(getBytes("batch-" + i), getBytes("batch-value-" + i));
      expectedResult.add("batch-" + i);
    }

    BatchOperation batch = new BatchOperation();
    batch.delete(getBytes("batch-2"));
    batch.delete(getBytes("batch-3"));
    batch.delete(getBytes("batch-4"));
    batch.put(getBytes("batch-new-2"), getBytes("batch-new-value-2"));

    expectedResult.remove("batch-2");
    expectedResult.remove("batch-3");
    expectedResult.remove("batch-4");
    expectedResult.add("batch-new-2");

    dbStore.writeBatch(batch);

    Iterator<String> it = expectedResult.iterator();
    AtomicInteger count = new AtomicInteger(0);
    dbStore.iterate(null, (key, value) -> {
      count.incrementAndGet();
      return it.hasNext() && it.next().equals(getString(key));
    });

    Assert.assertEquals(8, count.get());
  }
}
