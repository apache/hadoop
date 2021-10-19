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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.base.Ticker;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Tristate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * MetadataStore unit test for {@link LocalMetadataStore}.
 */
public class TestLocalMetadataStore extends MetadataStoreTestBase {


  private final static class LocalMSContract extends AbstractMSContract {

    private FileSystem fs;

    private LocalMSContract() throws IOException {
      this(new Configuration());
    }

    private LocalMSContract(Configuration config) throws IOException {
      fs = FileSystem.getLocal(config);
    }

    @Override
    public FileSystem getFileSystem() {
      return fs;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      LocalMetadataStore lms = new LocalMetadataStore();
      return lms;
    }
  }

  @Override
  public AbstractMSContract createContract() throws IOException {
    return new LocalMSContract();
  }

  @Override
  public AbstractMSContract createContract(Configuration conf) throws
      IOException {
    return new LocalMSContract(conf);
  }

  @Override protected String getPathStringForPrune(String path)
      throws Exception{
    return path;
  }

  @Test
  public void testClearByAncestor() throws Exception {
    Cache<Path, LocalMetadataEntry> cache = CacheBuilder.newBuilder().build();

    // 1. Test paths without scheme/host
    assertClearResult(cache, "", "/", 0);
    assertClearResult(cache, "", "/dirA/dirB", 2);
    assertClearResult(cache, "", "/invalid", 5);


    // 2. Test paths w/ scheme/host
    String p = "s3a://fake-bucket-name";
    assertClearResult(cache, p, "/", 0);
    assertClearResult(cache, p, "/dirA/dirB", 2);
    assertClearResult(cache, p, "/invalid", 5);
  }

  static class TestTicker extends Ticker {
    private long myTicker = 0;
    @Override public long read() {
      return myTicker;
    }
    public void set(long val) {
      this.myTicker = val;
    }

  }

  /**
   * Test that time eviction in cache used in {@link LocalMetadataStore}
   * implementation working properly.
   *
   * The test creates a Ticker instance, which will be used to control the
   * internal clock of the cache to achieve eviction without having to wait
   * for the system clock.
   * The test creates 3 entry: 2nd and 3rd entry will survive the eviction,
   * because it will be created later than the 1st - using the ticker.
   */
  @Test
  public void testCacheTimedEvictionAfterWrite() {
    TestTicker testTicker = new TestTicker();
    final long t0 = testTicker.read();
    final long t1 = t0 + 100;
    final long t2 = t1 + 100;

    final long ttl = t1 + 50; // between t1 and t2

    Cache<Path, LocalMetadataEntry> cache = CacheBuilder.newBuilder()
        .expireAfterWrite(ttl,
            TimeUnit.NANOSECONDS /* nanos to avoid conversions */)
        .ticker(testTicker)
        .build();

    String p = "s3a://fake-bucket-name";
    Path path1 = new Path(p + "/dirA/dirB/file1");
    Path path2 = new Path(p + "/dirA/dirB/file2");
    Path path3 = new Path(p + "/dirA/dirB/file3");

    // Test time is t0
    populateEntry(cache, path1);

    // set new value on the ticker, so the next two entries will be added later
    testTicker.set(t1);  // Test time is now t1
    populateEntry(cache, path2);
    populateEntry(cache, path3);

    assertEquals("Cache should contain 3 records before eviction",
        3, cache.size());
    LocalMetadataEntry pm1 = cache.getIfPresent(path1);
    assertNotNull("PathMetadata should not be null before eviction", pm1);

    // set the ticker to a time when timed eviction should occur
    // for the first entry
    testTicker.set(t2);

    // call cleanup explicitly, as timed expiration is performed with
    // periodic maintenance during writes and occasionally during reads only
    cache.cleanUp();

    assertEquals("Cache size should be 2 after eviction", 2, cache.size());
    pm1 = cache.getIfPresent(path1);
    assertNull("PathMetadata should be null after eviction", pm1);
  }


  @Test
  public void testUpdateParentLastUpdatedOnPutNewParent() throws Exception {
    Assume.assumeTrue("This test only applies if metadatastore does not allow"
        + " missing values (skip for NullMS).", !allowMissing());

    ITtlTimeProvider tp = mock(ITtlTimeProvider.class);
    ITtlTimeProvider originalTimeProvider = getTtlTimeProvider();

    long now = 100L;

    final String parent = "/parentUpdated-" + UUID.randomUUID();
    final String child = parent + "/file1";

    try {
      when(tp.getNow()).thenReturn(now);

      // create a file
      ms.put(new PathMetadata(makeFileStatus(child, 100), tp.getNow()),
          null);
      final PathMetadata fileMeta = ms.get(strToPath(child));
      assertEquals("lastUpdated field of first file should be equal to the "
          + "mocked value", now, fileMeta.getLastUpdated());

      final DirListingMetadata listing = ms.listChildren(strToPath(parent));
      assertEquals("Listing lastUpdated field should be equal to the mocked "
          + "time value.", now, listing.getLastUpdated());

    } finally {
      ms.setTtlTimeProvider(originalTimeProvider);
    }

  }


  private static void populateMap(Cache<Path, LocalMetadataEntry> cache,
      String prefix) {
    populateEntry(cache, new Path(prefix + "/dirA/dirB/"));
    populateEntry(cache, new Path(prefix + "/dirA/dirB/dirC"));
    populateEntry(cache, new Path(prefix + "/dirA/dirB/dirC/file1"));
    populateEntry(cache, new Path(prefix + "/dirA/dirB/dirC/file2"));
    populateEntry(cache, new Path(prefix + "/dirA/file1"));
  }

  private static void populateEntry(Cache<Path, LocalMetadataEntry> cache,
      Path path) {
    S3AFileStatus s3aStatus = new S3AFileStatus(Tristate.UNKNOWN, path, null);
    cache.put(path, new LocalMetadataEntry(new PathMetadata(s3aStatus)));
  }

  private static long sizeOfMap(Cache<Path, LocalMetadataEntry> cache) {
    return cache.asMap().values().stream()
        .filter(entry -> !entry.getFileMeta().isDeleted())
        .count();
  }

  private static void assertClearResult(Cache<Path, LocalMetadataEntry> cache,
      String prefixStr, String pathStr, int leftoverSize) throws IOException {
    populateMap(cache, prefixStr);
    LocalMetadataStore.deleteEntryByAncestor(new Path(prefixStr + pathStr),
        cache, true, getTtlTimeProvider());
    assertEquals(String.format("Cache should have %d entries", leftoverSize),
        leftoverSize, sizeOfMap(cache));
    cache.invalidateAll();
  }

  @Override
  protected void verifyFileStatus(FileStatus status, long size) {
    S3ATestUtils.verifyFileStatus(status, size, REPLICATION, getModTime(),
        getAccessTime(),
        BLOCK_SIZE, OWNER, GROUP, PERMISSION);
  }

  @Override
  protected void verifyDirStatus(S3AFileStatus status) {
    S3ATestUtils.verifyDirStatus(status, REPLICATION, OWNER);
  }

}
