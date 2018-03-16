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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link MountTableStore} from the {@link Router}.
 */
public class TestMountTableResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMountTableResolver.class);

  private static final int TEST_MAX_CACHE_SIZE = 10;

  private MountTableResolver mountTable;

  private Map<String, String> getMountTableEntry(
      String subcluster, String path) {
    Map<String, String> ret = new HashMap<>();
    ret.put(subcluster, path);
    return ret;
  }

  /**
   * Setup the mount table.
   * / -> 1:/
   * __tmp -> 2:/tmp
   * __user -> 3:/user
   * ____a -> 2:/user/test
   * ______demo
   * ________test
   * __________a -> 1:/user/test
   * __________b -> 3:/user/test
   * ____b
   * ______file1.txt -> 4:/user/file1.txt
   * __usr
   * ____bin -> 2:/bin
   * __readonly -> 2:/tmp
   *
   * @throws IOException If it cannot set the mount table.
   */
  private void setupMountTable() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(
        FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE, TEST_MAX_CACHE_SIZE);
    conf.setStrings(DFS_ROUTER_DEFAULT_NAMESERVICE, "0");
    mountTable = new MountTableResolver(conf);

    // Root mount point
    Map<String, String> map = getMountTableEntry("1", "/");
    mountTable.addEntry(MountTable.newInstance("/", map));

    // /tmp
    map = getMountTableEntry("2", "/");
    mountTable.addEntry(MountTable.newInstance("/tmp", map));

    // /user
    map = getMountTableEntry("3", "/user");
    mountTable.addEntry(MountTable.newInstance("/user", map));

    // /usr/bin
    map = getMountTableEntry("2", "/bin");
    mountTable.addEntry(MountTable.newInstance("/usr/bin", map));

    // /user/a
    map = getMountTableEntry("2", "/user/test");
    mountTable.addEntry(MountTable.newInstance("/user/a", map));

    // /user/b/file1.txt
    map = getMountTableEntry("4", "/user/file1.txt");
    mountTable.addEntry(MountTable.newInstance("/user/b/file1.txt", map));

    // /user/a/demo/test/a
    map = getMountTableEntry("1", "/user/test");
    mountTable.addEntry(MountTable.newInstance("/user/a/demo/test/a", map));

    // /user/a/demo/test/b
    map = getMountTableEntry("3", "/user/test");
    mountTable.addEntry(MountTable.newInstance("/user/a/demo/test/b", map));

    // /readonly
    map = getMountTableEntry("2", "/tmp");
    MountTable readOnlyEntry = MountTable.newInstance("/readonly", map);
    readOnlyEntry.setReadOnly(true);
    mountTable.addEntry(readOnlyEntry);
  }

  @Before
  public void setup() throws IOException {
    setupMountTable();
  }

  @Test
  public void testDestination() throws IOException {

    // Check files
    assertEquals("1->/tesfile1.txt",
        mountTable.getDestinationForPath("/tesfile1.txt").toString());

    assertEquals("3->/user/testfile2.txt",
        mountTable.getDestinationForPath("/user/testfile2.txt").toString());

    assertEquals("2->/user/test/testfile3.txt",
        mountTable.getDestinationForPath("/user/a/testfile3.txt").toString());

    assertEquals("3->/user/b/testfile4.txt",
        mountTable.getDestinationForPath("/user/b/testfile4.txt").toString());

    assertEquals("1->/share/file5.txt",
        mountTable.getDestinationForPath("/share/file5.txt").toString());

    assertEquals("2->/bin/file7.txt",
        mountTable.getDestinationForPath("/usr/bin/file7.txt").toString());

    assertEquals("1->/usr/file8.txt",
        mountTable.getDestinationForPath("/usr/file8.txt").toString());

    assertEquals("2->/user/test/demo/file9.txt",
        mountTable.getDestinationForPath("/user/a/demo/file9.txt").toString());

    // Check folders
    assertEquals("3->/user/testfolder",
        mountTable.getDestinationForPath("/user/testfolder").toString());

    assertEquals("2->/user/test/b",
        mountTable.getDestinationForPath("/user/a/b").toString());

    assertEquals("3->/user/test/a",
        mountTable.getDestinationForPath("/user/test/a").toString());

    assertEquals("2->/tmp/tesfile1.txt",
        mountTable.getDestinationForPath("/readonly/tesfile1.txt").toString());

  }

  private void compareLists(List<String> list1, String[] list2) {
    assertEquals(list1.size(), list2.length);
    for (String item : list2) {
      assertTrue(list1.contains(item));
    }
  }

  @Test
  public void testGetMountPoint() throws IOException {
    // Check get the mount table entry for a path
    MountTable mtEntry;
    mtEntry = mountTable.getMountPoint("/");
    assertTrue(mtEntry.getSourcePath().equals("/"));

    mtEntry = mountTable.getMountPoint("/user");
    assertTrue(mtEntry.getSourcePath().equals("/user"));

    mtEntry = mountTable.getMountPoint("/user/a");
    assertTrue(mtEntry.getSourcePath().equals("/user/a"));

    mtEntry = mountTable.getMountPoint("/user/a/");
    assertTrue(mtEntry.getSourcePath().equals("/user/a"));

    mtEntry = mountTable.getMountPoint("/user/a/11");
    assertTrue(mtEntry.getSourcePath().equals("/user/a"));

    mtEntry = mountTable.getMountPoint("/user/a1");
    assertTrue(mtEntry.getSourcePath().equals("/user"));
  }

  @Test
  public void testGetMountPoints() throws IOException {

    // Check getting all mount points (virtual and real) beneath a path
    List<String> mounts = mountTable.getMountPoints("/");
    assertEquals(4, mounts.size());
    compareLists(mounts, new String[] {"tmp", "user", "usr", "readonly"});

    mounts = mountTable.getMountPoints("/user");
    assertEquals(2, mounts.size());
    compareLists(mounts, new String[] {"a", "b"});

    mounts = mountTable.getMountPoints("/user/a");
    assertEquals(1, mounts.size());
    compareLists(mounts, new String[] {"demo"});

    mounts = mountTable.getMountPoints("/user/a/demo");
    assertEquals(1, mounts.size());
    compareLists(mounts, new String[] {"test"});

    mounts = mountTable.getMountPoints("/user/a/demo/test");
    assertEquals(2, mounts.size());
    compareLists(mounts, new String[] {"a", "b"});

    mounts = mountTable.getMountPoints("/tmp");
    assertEquals(0, mounts.size());

    mounts = mountTable.getMountPoints("/t");
    assertNull(mounts);

    mounts = mountTable.getMountPoints("/unknownpath");
    assertNull(mounts);
  }

  private void compareRecords(List<MountTable> list1, String[] list2) {
    assertEquals(list1.size(), list2.length);
    for (String item : list2) {
      for (MountTable record : list1) {
        if (record.getSourcePath().equals(item)) {
          return;
        }
      }
    }
    fail();
  }

  @Test
  public void testGetMounts() throws IOException {

    // Check listing the mount table records at or beneath a path
    List<MountTable> records = mountTable.getMounts("/");
    assertEquals(9, records.size());
    compareRecords(records, new String[] {"/", "/tmp", "/user", "/usr/bin",
        "user/a", "/user/a/demo/a", "/user/a/demo/b", "/user/b/file1.txt",
        "readonly"});

    records = mountTable.getMounts("/user");
    assertEquals(5, records.size());
    compareRecords(records, new String[] {"/user", "/user/a/demo/a",
        "/user/a/demo/b", "user/a", "/user/b/file1.txt"});

    records = mountTable.getMounts("/user/a");
    assertEquals(3, records.size());
    compareRecords(records,
        new String[] {"/user/a/demo/a", "/user/a/demo/b", "/user/a"});

    records = mountTable.getMounts("/tmp");
    assertEquals(1, records.size());
    compareRecords(records, new String[] {"/tmp"});

    records = mountTable.getMounts("/readonly");
    assertEquals(1, records.size());
    compareRecords(records, new String[] {"/readonly"});
    assertTrue(records.get(0).isReadOnly());
  }

  @Test
  public void testRemoveSubTree()
      throws UnsupportedOperationException, IOException {

    // 3 mount points are present /tmp, /user, /usr
    compareLists(mountTable.getMountPoints("/"),
        new String[] {"user", "usr", "tmp", "readonly"});

    // /tmp currently points to namespace 2
    assertEquals("2", mountTable.getDestinationForPath("/tmp/testfile.txt")
        .getDefaultLocation().getNameserviceId());

    // Remove tmp
    mountTable.removeEntry("/tmp");

    // Now 2 mount points are present /user, /usr
    compareLists(mountTable.getMountPoints("/"),
        new String[] {"user", "usr", "readonly"});

    // /tmp no longer exists, uses default namespace for mapping /
    assertEquals("1", mountTable.getDestinationForPath("/tmp/testfile.txt")
        .getDefaultLocation().getNameserviceId());
  }

  @Test
  public void testRemoveVirtualNode()
      throws UnsupportedOperationException, IOException {

    // 3 mount points are present /tmp, /user, /usr
    compareLists(mountTable.getMountPoints("/"),
        new String[] {"user", "usr", "tmp", "readonly"});

    // /usr is virtual, uses namespace 1->/
    assertEquals("1", mountTable.getDestinationForPath("/usr/testfile.txt")
        .getDefaultLocation().getNameserviceId());

    // Attempt to remove /usr
    mountTable.removeEntry("/usr");

    // Verify the remove failed
    compareLists(mountTable.getMountPoints("/"),
        new String[] {"user", "usr", "tmp", "readonly"});
  }

  @Test
  public void testRemoveLeafNode()
      throws UnsupportedOperationException, IOException {

    // /user/a/demo/test/a currently points to namespace 1
    assertEquals("1", mountTable.getDestinationForPath("/user/a/demo/test/a")
        .getDefaultLocation().getNameserviceId());

    // Remove /user/a/demo/test/a
    mountTable.removeEntry("/user/a/demo/test/a");

    // Now /user/a/demo/test/a points to namespace 2 using the entry for /user/a
    assertEquals("2", mountTable.getDestinationForPath("/user/a/demo/test/a")
        .getDefaultLocation().getNameserviceId());

    // Verify the virtual node at /user/a/demo still exists and was not deleted
    compareLists(mountTable.getMountPoints("/user/a"), new String[] {"demo"});

    // Verify the sibling node was unaffected and still points to ns 3
    assertEquals("3", mountTable.getDestinationForPath("/user/a/demo/test/b")
        .getDefaultLocation().getNameserviceId());
  }

  @Test
  public void testRefreshEntries()
      throws UnsupportedOperationException, IOException {

    // Initial table loaded
    testDestination();
    assertEquals(9, mountTable.getMounts("/").size());

    // Replace table with /1 and /2
    List<MountTable> records = new ArrayList<>();
    Map<String, String> map1 = getMountTableEntry("1", "/");
    records.add(MountTable.newInstance("/1", map1));
    Map<String, String> map2 = getMountTableEntry("2", "/");
    records.add(MountTable.newInstance("/2", map2));
    mountTable.refreshEntries(records);

    // Verify addition
    PathLocation destination1 = mountTable.getDestinationForPath("/1");
    RemoteLocation defaultLoc1 = destination1.getDefaultLocation();
    assertEquals("1", defaultLoc1.getNameserviceId());

    PathLocation destination2 = mountTable.getDestinationForPath("/2");
    RemoteLocation defaultLoc2 = destination2.getDefaultLocation();
    assertEquals("2", defaultLoc2.getNameserviceId());

    // Verify existing entries were removed
    assertEquals(2, mountTable.getMounts("/").size());
    boolean assertionThrown = false;
    try {
      testDestination();
      fail();
    } catch (AssertionError e) {
      // The / entry was removed, so it triggers an exception
      assertionThrown = true;
    }
    assertTrue(assertionThrown);
  }

  @Test
  public void testMountTableScalability() throws IOException {

    List<MountTable> emptyList = new ArrayList<>();
    mountTable.refreshEntries(emptyList);

    // Add 100,000 entries in flat list
    for (int i = 0; i < 100000; i++) {
      Map<String, String> map = getMountTableEntry("1", "/" + i);
      MountTable record = MountTable.newInstance("/" + i, map);
      mountTable.addEntry(record);
      if (i % 10000 == 0) {
        LOG.info("Adding flat mount record {}: {}", i, record);
      }
    }

    assertEquals(100000, mountTable.getMountPoints("/").size());
    assertEquals(100000, mountTable.getMounts("/").size());

    // Add 1000 entries in deep list
    mountTable.refreshEntries(emptyList);
    String parent = "/";
    for (int i = 0; i < 1000; i++) {
      final int index = i;
      Map<String, String> map = getMountTableEntry("1", "/" + index);
      if (i > 0) {
        parent = parent + "/";
      }
      parent = parent + i;
      MountTable record = MountTable.newInstance(parent, map);
      mountTable.addEntry(record);
    }

    assertEquals(1, mountTable.getMountPoints("/").size());
    assertEquals(1000, mountTable.getMounts("/").size());

    // Add 100,000 entries in deep and wide tree
    mountTable.refreshEntries(emptyList);
    Random rand = new Random();
    parent = "/" + Integer.toString(rand.nextInt());
    int numRootTrees = 1;
    for (int i = 0; i < 100000; i++) {
      final int index = i;
      Map<String, String> map = getMountTableEntry("1", "/" + index);
      parent = parent + "/" + i;
      if (parent.length() > 2000) {
        // Start new tree
        parent = "/" + Integer.toString(rand.nextInt());
        numRootTrees++;
      }
      MountTable record = MountTable.newInstance(parent, map);
      mountTable.addEntry(record);
    }

    assertEquals(numRootTrees, mountTable.getMountPoints("/").size());
    assertEquals(100000, mountTable.getMounts("/").size());
  }

  @Test
  public void testUpdate() throws IOException {

    // Add entry to update later
    Map<String, String> map = getMountTableEntry("1", "/");
    mountTable.addEntry(MountTable.newInstance("/testupdate", map));

    MountTable entry = mountTable.getMountPoint("/testupdate");
    List<RemoteLocation> dests = entry.getDestinations();
    assertEquals(1, dests.size());
    RemoteLocation dest = dests.get(0);
    assertEquals("1", dest.getNameserviceId());

    // Update entry
    Collection<MountTable> entries = Collections.singletonList(
        MountTable.newInstance("/testupdate", getMountTableEntry("2", "/")));
    mountTable.refreshEntries(entries);

    MountTable entry1 = mountTable.getMountPoint("/testupdate");
    List<RemoteLocation> dests1 = entry1.getDestinations();
    assertEquals(1, dests1.size());
    RemoteLocation dest1 = dests1.get(0);
    assertEquals("2", dest1.getNameserviceId());

    // Remove the entry to test updates and check
    mountTable.removeEntry("/testupdate");
    MountTable entry2 = mountTable.getMountPoint("/testupdate");
    assertNull(entry2);
  }

  @Test
  public void testCacheCleaning() throws Exception {
    for (int i = 0; i < 1000; i++) {
      String filename = String.format("/user/a/file-%04d.txt", i);
      mountTable.getDestinationForPath(filename);
    }
    long cacheSize = mountTable.getCacheSize();
    assertTrue(cacheSize <= TEST_MAX_CACHE_SIZE);
  }

  @Test
  public void testLocationCache() throws Exception {
    List<MountTable> entries = new ArrayList<>();

    // Add entry and test location cache
    Map<String, String> map1 = getMountTableEntry("1", "/testlocationcache");
    MountTable entry1 = MountTable.newInstance("/testlocationcache", map1);
    entries.add(entry1);

    Map<String, String> map2 = getMountTableEntry("2",
            "/anothertestlocationcache");
    MountTable entry2 = MountTable.newInstance("/anothertestlocationcache",
            map2);
    entries.add(entry2);
    mountTable.refreshEntries(entries);
    assertEquals("1->/testlocationcache/",
            mountTable.getDestinationForPath("/testlocationcache").toString());
    assertEquals("2->/anothertestlocationcache/",
            mountTable.getDestinationForPath("/anothertestlocationcache")
                    .toString());

    // Remove the entry1
    entries.remove(entry1);
    mountTable.refreshEntries(entries);

    // Add the default location and test location cache
    assertEquals("0->/testlocationcache",
            mountTable.getDestinationForPath("/testlocationcache").toString());

    // Add the entry again but mount to another ns
    Map<String, String> map3 = getMountTableEntry("3", "/testlocationcache");
    MountTable entry3 = MountTable.newInstance("/testlocationcache", map3);
    entries.add(entry3);
    mountTable.refreshEntries(entries);

    // Ensure location cache update correctly
    assertEquals("3->/testlocationcache/",
            mountTable.getDestinationForPath("/testlocationcache").toString());

    // Cleanup before exit
    mountTable.removeEntry("/testlocationcache");
    mountTable.removeEntry("/anothertestlocationcache");
  }
}