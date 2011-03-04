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

package org.apache.hadoop.filecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

public class TestTrackerDistributedCacheManager extends TestCase {
  private static final String TEST_LOCAL_DIR_PROP = "test.local.dir";
  private static String TEST_CACHE_BASE_DIR =
    new Path(System.getProperty("test.build.data","/tmp/cachebasedir"))
    .toString().replace(' ', '+');
  private static String TEST_ROOT_DIR =
    System.getProperty("test.build.data", "/tmp/distributedcache");
  private static final int TEST_FILE_SIZE = 4 * 1024; // 4K
  private static final int LOCAL_CACHE_LIMIT = 5 * 1024; //5K
  private Configuration conf;
  private Path firstCacheFile;
  private Path secondCacheFile;

  @Override
  protected void setUp() throws IOException {
    conf = new Configuration();
    conf.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
    conf.set(TEST_LOCAL_DIR_PROP, TEST_ROOT_DIR);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    createTempFile(firstCacheFile);
    createTempFile(secondCacheFile);
  }

  /**
   * This is the typical flow for using the DistributedCache classes.
   */
  public void testManagerFlow() throws IOException {
    TrackerDistributedCacheManager manager = 
        new TrackerDistributedCacheManager(conf);
    LocalDirAllocator localDirAllocator = 
        new LocalDirAllocator(TEST_LOCAL_DIR_PROP);

    // Configures a task/job with both a regular file and a "classpath" file.
    Configuration subConf = new Configuration(conf);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf);
    DistributedCache.addFileToClassPath(secondCacheFile, subConf);
    TrackerDistributedCacheManager.determineTimestamps(subConf);

    Path jobFile = new Path(TEST_ROOT_DIR, "job.xml");
    FileOutputStream os = new FileOutputStream(new File(jobFile.toString()));
    subConf.writeXml(os);
    os.close();

    TaskDistributedCacheManager handle =
      manager.newTaskDistributedCacheManager(subConf);
    assertNull(null, DistributedCache.getLocalCacheFiles(subConf));
    handle.setup(localDirAllocator, 
        new File(new Path(TEST_ROOT_DIR, "workdir").toString()), "distcache");
    Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(subConf);
    assertNotNull(null, localCacheFiles);
    assertEquals(2, localCacheFiles.length);
    Path cachedFirstFile = localCacheFiles[0];
    Path cachedSecondFile = localCacheFiles[1];
    assertFileLengthEquals(firstCacheFile, cachedFirstFile);
    assertFalse("Paths should be different.", 
        firstCacheFile.equals(cachedFirstFile));

    assertEquals(1, handle.getClassPaths().size());
    assertEquals(cachedSecondFile.toString(), handle.getClassPaths().get(0));

    // Cleanup
    handle.release();
    manager.purgeCache();
    assertFalse(pathToFile(cachedFirstFile).exists());
  }


  /** test delete cache */
  public void testDeleteCache() throws Exception {
    TrackerDistributedCacheManager manager = 
        new TrackerDistributedCacheManager(conf);
    FileSystem localfs = FileSystem.getLocal(conf);

    manager.getLocalCache(firstCacheFile.toUri(), conf, 
        new Path(TEST_CACHE_BASE_DIR), null, false, 
        System.currentTimeMillis(), new Path(TEST_ROOT_DIR), false);
    manager.releaseCache(firstCacheFile.toUri(), conf);
    //in above code,localized a file of size 4K and then release the cache 
    // which will cause the cache be deleted when the limit goes out. 
    // The below code localize another cache which's designed to
    //sweep away the first cache.
    manager.getLocalCache(secondCacheFile.toUri(), conf, 
        new Path(TEST_CACHE_BASE_DIR), null, false, 
        System.currentTimeMillis(), new Path(TEST_ROOT_DIR), false);
    FileStatus[] dirStatuses = localfs.listStatus(
        new Path(TEST_CACHE_BASE_DIR));
    assertTrue("DistributedCache failed deleting old" + 
        " cache when the cache store is full.",
        dirStatuses.length > 1);
  }
  
  public void testFileSystemOtherThanDefault() throws Exception {
    TrackerDistributedCacheManager manager =
      new TrackerDistributedCacheManager(conf);
    conf.set("fs.fakefile.impl", conf.get("fs.file.impl"));
    Path fileToCache = new Path("fakefile:///"
        + firstCacheFile.toUri().getPath());
    Path result = manager.getLocalCache(fileToCache.toUri(), conf,
        new Path(TEST_CACHE_BASE_DIR), null, false, System.currentTimeMillis(),
        new Path(TEST_ROOT_DIR), false);
    assertNotNull("DistributedCache cached file on non-default filesystem.",
        result);
  }

  static void createTempFile(Path p) throws IOException {
    File f = new File(p.toString());
    FileOutputStream os = new FileOutputStream(f);
    byte[] toWrite = new byte[TEST_FILE_SIZE];
    new Random().nextBytes(toWrite);
    os.write(toWrite);
    os.close();
    FileSystem.LOG.info("created: " + p + ", size=" + TEST_FILE_SIZE);
  }

  @Override
  protected void tearDown() throws IOException {
    new File(firstCacheFile.toString()).delete();
    new File(secondCacheFile.toString()).delete();
  }

  private void assertFileLengthEquals(Path a, Path b) 
      throws FileNotFoundException {
    assertEquals("File sizes mismatch.", 
       pathToFile(a).length(), pathToFile(b).length());
  }

  private File pathToFile(Path p) {
    return new File(p.toString());
  }
}
