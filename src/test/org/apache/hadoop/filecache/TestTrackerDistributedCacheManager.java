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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.DefaultTaskController;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskController.InitializationContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.security.UserGroupInformation;


public class TestTrackerDistributedCacheManager extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestTrackerDistributedCacheManager.class);

  protected String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          TestTrackerDistributedCacheManager.class.getSimpleName())
          .getAbsolutePath();

  protected File ROOT_MAPRED_LOCAL_DIR;
  private static String TEST_CACHE_BASE_DIR = "cachebasedir";
  protected int numLocalDirs = 6;

  private static final int TEST_FILE_SIZE = 4 * 1024; // 4K
  private static final int LOCAL_CACHE_LIMIT = 5 * 1024; //5K
  protected Configuration conf;
  protected Path firstCacheFile;
  protected Path secondCacheFile;
  private FileSystem fs;

  protected LocalDirAllocator localDirAllocator = 
    new LocalDirAllocator(JobConf.MAPRED_LOCAL_DIR_PROPERTY);

  @Override
  protected void setUp() throws IOException {

    // Prepare the tests' root dir
    File TEST_ROOT = new File(TEST_ROOT_DIR);
    if (!TEST_ROOT.exists()) {
      TEST_ROOT.mkdirs();
    }

    // Prepare the tests' mapred-local-dir
    ROOT_MAPRED_LOCAL_DIR = new File(TEST_ROOT_DIR, "mapred/local");
    ROOT_MAPRED_LOCAL_DIR.mkdirs();

    conf = new Configuration();
    conf.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
    conf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY,
             ROOT_MAPRED_LOCAL_DIR.toString());
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    fs = FileSystem.get(conf);

    // Create the temporary cache files to be used in the tests.
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    createTempFile(firstCacheFile);
    createTempFile(secondCacheFile);
  }

  /**
   * Whether the test can run on the machine
   * 
   * @return true if test can run on the machine, false otherwise
   */
  protected boolean canRun() {
    return true;
  }
  
  /**
   * This is the typical flow for using the DistributedCache classes.
   * 
   * @throws IOException
   * @throws LoginException
   */
  public void testManagerFlow() throws IOException, LoginException {
    if (!canRun()) {
      return;
    }

    // ****** Imitate JobClient code
    // Configures a task/job with both a regular file and a "classpath" file.
    Configuration subConf = new Configuration(conf);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf);
    DistributedCache.addFileToClassPath(secondCacheFile, subConf);
    TrackerDistributedCacheManager.determineTimestamps(subConf);
    // ****** End of imitating JobClient code

    Path jobFile = new Path(TEST_ROOT_DIR, "job.xml");
    FileOutputStream os = new FileOutputStream(new File(jobFile.toString()));
    subConf.writeXml(os);
    os.close();

    String userName = getJobOwnerName();

    // ****** Imitate TaskRunner code.
    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(conf);
    TaskDistributedCacheManager handle =
      manager.newTaskDistributedCacheManager(subConf);
    assertNull(null, DistributedCache.getLocalCacheFiles(subConf));
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());
    handle.setup(localDirAllocator, workDir, TaskTracker
        .getDistributedCacheDir(userName));

    InitializationContext context = new InitializationContext();
    context.user = userName;
    context.workDir = workDir;
    getTaskController().initializeDistributedCache(context);
    // ****** End of imitating TaskRunner code

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

    checkFilePermissions(localCacheFiles);

    // Cleanup
    handle.release();
    manager.purgeCache();
    assertFalse(pathToFile(cachedFirstFile).exists());
  }

  /**
   * This DistributedCacheManager fails in localizing firstCacheFile.
   */
  public class FakeTrackerDistributedCacheManager extends
      TrackerDistributedCacheManager {
    public FakeTrackerDistributedCacheManager(Configuration conf)
        throws IOException {
      super(conf);
    }

    @Override
    Path localizeCache(Configuration conf, URI cache, long confFileStamp,
        CacheStatus cacheStatus, FileStatus fileStatus, boolean isArchive)
        throws IOException {
      if (cache.equals(firstCacheFile.toUri())) {
        throw new IOException("fake fail");
      }
      return super.localizeCache(conf, cache, confFileStamp, cacheStatus,
          fileStatus, isArchive);
    }
  }

  public void testReferenceCount() throws IOException, LoginException,
      URISyntaxException {
    if (!canRun()) {
      return;
    }
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    TrackerDistributedCacheManager manager = 
      new FakeTrackerDistributedCacheManager(conf);

    String userName = getJobOwnerName();
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());

    // Configures a job with a regular file
    Job job1 = new Job(conf);
    Configuration conf1 = job1.getConfiguration();
    DistributedCache.addCacheFile(secondCacheFile.toUri(), conf1);
    
    TrackerDistributedCacheManager.determineTimestamps(conf1);

    // Task localizing for first job
    TaskDistributedCacheManager handle = manager
        .newTaskDistributedCacheManager(conf1);
    handle.setup(localDirAllocator, workDir, TaskTracker
          .getDistributedCacheDir(userName));
    handle.release();
    for (TaskDistributedCacheManager.CacheFile c : handle.getCacheFiles()) {
      assertEquals(0, manager.getReferenceCount(c.uri, conf1, c.timestamp));
    }
    
    Path thirdCacheFile = new Path(TEST_ROOT_DIR, "thirdcachefile");
    createTempFile(thirdCacheFile);
    
    // Configures another job with three regular files.
    Job job2 = new Job(conf);
    Configuration conf2 = job2.getConfiguration();
    // add a file that would get failed to localize
    DistributedCache.addCacheFile(firstCacheFile.toUri(), conf2);
    // add a file that is already localized by different job
    DistributedCache.addCacheFile(secondCacheFile.toUri(), conf2);
    // add a file that is never localized
    DistributedCache.addCacheFile(thirdCacheFile.toUri(), conf2);
    
    TrackerDistributedCacheManager.determineTimestamps(conf2);

    // Task localizing for second job
    // localization for the "firstCacheFile" will fail.
    handle = manager.newTaskDistributedCacheManager(conf2);
    Throwable th = null;
    try {
      handle.setup(localDirAllocator, workDir, TaskTracker
          .getDistributedCacheDir(userName));
    } catch (IOException e) {
      th = e;
      LOG.info("Exception during setup", e);
    }
    assertNotNull(th);
    assertTrue(th.getMessage().contains("fake fail"));
    handle.release();
    th = null;
    for (TaskDistributedCacheManager.CacheFile c : handle.getCacheFiles()) {
      try {
        assertEquals(0, manager.getReferenceCount(c.uri, conf2, c.timestamp));
      } catch (IOException ie) {
        th = ie;
        LOG.info("Exception getting reference count for " + c.uri, ie);
      }
    }
    assertNotNull(th);
    assertTrue(th.getMessage().contains(thirdCacheFile.getName()));
    fs.delete(thirdCacheFile, false);
  }

  /**
   * Check proper permissions on the cache files
   * 
   * @param localCacheFiles
   * @throws IOException
   */
  protected void checkFilePermissions(Path[] localCacheFiles)
      throws IOException {
    Path cachedFirstFile = localCacheFiles[0];
    Path cachedSecondFile = localCacheFiles[1];
    // Both the files should have executable permissions on them.
    assertTrue("First cache file is not executable!", new File(cachedFirstFile
        .toUri().getPath()).canExecute());
    assertTrue("Second cache file is not executable!", new File(
        cachedSecondFile.toUri().getPath()).canExecute());
  }

  protected TaskController getTaskController() {
    return new DefaultTaskController();
  }

  protected String getJobOwnerName() throws LoginException {
    UserGroupInformation ugi = UserGroupInformation.login(conf);
    return ugi.getUserName();
  }

  /** test delete cache */
  public void testDeleteCache() throws Exception {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager = 
        new TrackerDistributedCacheManager(conf);
    FileSystem localfs = FileSystem.getLocal(conf);
    long now = System.currentTimeMillis();

    manager.getLocalCache(firstCacheFile.toUri(), conf, 
        TEST_CACHE_BASE_DIR, fs.getFileStatus(firstCacheFile), false,
        now, new Path(TEST_ROOT_DIR), false);
    manager.releaseCache(firstCacheFile.toUri(), conf, now);
    //in above code,localized a file of size 4K and then release the cache 
    // which will cause the cache be deleted when the limit goes out. 
    // The below code localize another cache which's designed to
    //sweep away the first cache.
    manager.getLocalCache(secondCacheFile.toUri(), conf, 
        TEST_CACHE_BASE_DIR, fs.getFileStatus(secondCacheFile), false, 
        System.currentTimeMillis(), new Path(TEST_ROOT_DIR), false);
    FileStatus[] dirStatuses = localfs.listStatus(
      new Path(ROOT_MAPRED_LOCAL_DIR.toString()));
    assertTrue("DistributedCache failed deleting old" + 
        " cache when the cache store is full.",
        dirStatuses.length == 1);
  }
  
  public void testFileSystemOtherThanDefault() throws Exception {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager =
      new TrackerDistributedCacheManager(conf);
    conf.set("fs.fakefile.impl", conf.get("fs.file.impl"));
    Path fileToCache = new Path("fakefile:///"
        + firstCacheFile.toUri().getPath());
    Path result = manager.getLocalCache(fileToCache.toUri(), conf,
        TEST_CACHE_BASE_DIR, fs.getFileStatus(firstCacheFile), false,
        System.currentTimeMillis(),
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
    FileUtil.fullyDelete(new File(TEST_ROOT_DIR));
  }

  protected void assertFileLengthEquals(Path a, Path b) 
      throws FileNotFoundException {
    assertEquals("File sizes mismatch.", 
       pathToFile(a).length(), pathToFile(b).length());
  }

  protected File pathToFile(Path p) {
    return new File(p.toString());
  }
  
  public static class FakeFileSystem extends RawLocalFileSystem {
    private long increment = 0;
    public FakeFileSystem() {
      super();
    }
    
    public FileStatus getFileStatus(Path p) throws IOException {
      File f = pathToFile(p);
      return new FileStatus(f.length(), f.isDirectory(), 1, 128,
      f.lastModified() + increment, makeQualified(new Path(f.getPath())));
    }
    
    void advanceClock(long millis) {
      increment += millis;
    }
  }
  
  public void testFreshness() throws Exception {
    if (!canRun()) {
      return;
    }
    Configuration myConf = new Configuration(conf);
    myConf.set("fs.default.name", "refresh:///");
    myConf.setClass("fs.refresh.impl", FakeFileSystem.class, FileSystem.class);
    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(myConf);
    // ****** Imitate JobClient code
    // Configures a task/job with both a regular file and a "classpath" file.
    Configuration subConf = new Configuration(myConf);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf);
    TrackerDistributedCacheManager.determineTimestamps(subConf);
    // ****** End of imitating JobClient code

    String userName = getJobOwnerName();

    // ****** Imitate TaskRunner code.
    TaskDistributedCacheManager handle =
      manager.newTaskDistributedCacheManager(subConf);
    assertNull(null, DistributedCache.getLocalCacheFiles(subConf));
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());
    handle.setup(localDirAllocator, workDir, TaskTracker
        .getDistributedCacheDir(userName));
    // ****** End of imitating TaskRunner code

    Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(subConf);
    assertNotNull(null, localCacheFiles);
    assertEquals(1, localCacheFiles.length);
    Path cachedFirstFile = localCacheFiles[0];
    assertFileLengthEquals(firstCacheFile, cachedFirstFile);
    assertFalse("Paths should be different.", 
        firstCacheFile.equals(cachedFirstFile));
    // release
    handle.release();
    
    // change the file timestamp
    FileSystem fs = FileSystem.get(myConf);
    ((FakeFileSystem)fs).advanceClock(1);

    // running a task of the same job
    Throwable th = null;
    try {
      handle.setup(localDirAllocator, workDir, TaskTracker
          .getDistributedCacheDir(userName));
    } catch (IOException ie) {
      th = ie;
    }
    assertNotNull("Throwable is null", th);
    assertTrue("Exception message does not match",
        th.getMessage().contains("has changed on HDFS since job started"));
    // release
    handle.release();
    
    // submit another job
    Configuration subConf2 = new Configuration(myConf);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf2);
    TrackerDistributedCacheManager.determineTimestamps(subConf2);
    
    handle =
      manager.newTaskDistributedCacheManager(subConf2);
    handle.setup(localDirAllocator, workDir, TaskTracker
        .getDistributedCacheDir(userName));
    Path[] localCacheFiles2 = DistributedCache.getLocalCacheFiles(subConf2);
    assertNotNull(null, localCacheFiles2);
    assertEquals(1, localCacheFiles2.length);
    Path cachedFirstFile2 = localCacheFiles2[0];
    assertFileLengthEquals(firstCacheFile, cachedFirstFile2);
    assertFalse("Paths should be different.", 
        firstCacheFile.equals(cachedFirstFile2));
    
    // assert that two localizations point to different paths
    assertFalse("two jobs with different timestamps did not localize" +
        " in different paths", cachedFirstFile.equals(cachedFirstFile2));
    // release
    handle.release();
  }

}
