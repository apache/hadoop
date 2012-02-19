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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.TaskDistributedCacheManager.CacheFile;
import org.apache.hadoop.mapred.DefaultTaskController;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobLocalizer;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

public class TestTrackerDistributedCacheManager extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestTrackerDistributedCacheManager.class);

  protected String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          TestTrackerDistributedCacheManager.class.getSimpleName())
          .getAbsolutePath();

  protected File ROOT_MAPRED_LOCAL_DIR;
  protected int numLocalDirs = 6;

  private static final int TEST_FILE_SIZE = 4 * 1024; // 4K
  private static final int LOCAL_CACHE_LIMIT = 5 * 1024; //5K
  private static final int LOCAL_CACHE_SUBDIR_LIMIT = 1;
  protected Configuration conf;
  protected Path firstCacheFile;
  protected Path firstCacheFilePublic;
  protected Path secondCacheFile;
  protected Path secondCacheFilePublic;
  protected Path firstCacheDirPublic;
  protected Path firstCacheDirPrivate;
  protected Path firstCacheFileInDirPublic;
  protected Path firstCacheFileInDirPrivate;
  private FileSystem fs;

  protected LocalDirAllocator localDirAllocator = 
    new LocalDirAllocator("mapred.local.dir");
  protected TaskController taskController;

  @Override
  protected void setUp() throws IOException,InterruptedException {

    // Prepare the tests' root dir
    File TEST_ROOT = new File(TEST_ROOT_DIR);
    if (!TEST_ROOT.exists()) {
      TEST_ROOT.mkdirs();
    }

    // Prepare the tests' mapred-local-dir
    ROOT_MAPRED_LOCAL_DIR = new File(TEST_ROOT_DIR, "mapred/local");
    ROOT_MAPRED_LOCAL_DIR.mkdirs();

    String []localDirs = new String[numLocalDirs];
    for (int i = 0; i < numLocalDirs; i++) {
      File localDir = new File(ROOT_MAPRED_LOCAL_DIR, "0_" + i);
      localDirs[i] = localDir.getPath();
      localDir.mkdir();
    }

    conf = new Configuration();
    conf.setStrings("mapred.local.dir", localDirs);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    fs = FileSystem.get(conf);
    Class<? extends TaskController> taskControllerClass = conf.getClass(
        "mapred.task.tracker.task-controller", DefaultTaskController.class,
        TaskController.class);
    taskController = (TaskController) ReflectionUtils.newInstance(
        taskControllerClass, conf);

    // setup permissions for mapred local dir
    UtilsForTests.setupTC(taskController, localDirAllocator,
        conf.getStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));

    // Create the temporary cache files to be used in the tests.
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    firstCacheFilePublic = new Path(TEST_ROOT_DIR, "firstcachefileOne");
    secondCacheFilePublic = new Path(TEST_ROOT_DIR, "secondcachefileOne");
    createPublicTempFile(firstCacheFilePublic);
    createPublicTempFile(secondCacheFilePublic);
    createPrivateTempFile(firstCacheFile);
    createPrivateTempFile(secondCacheFile);

    firstCacheDirPublic = new Path(TEST_ROOT_DIR, "firstcachedirPublic");
    firstCacheDirPrivate = new Path(TEST_ROOT_DIR, "firstcachedirPrivate");
    firstCacheFileInDirPublic = new Path(firstCacheDirPublic, "firstcacheFileinDirPublic.txt");
    firstCacheFileInDirPrivate = new Path(firstCacheDirPrivate, "firstcacheFileinDirPrivate.txt");
    createPublicTempDir(firstCacheDirPublic);
    createPrivateTempDir(firstCacheDirPrivate);
    createPublicTempFile(firstCacheFileInDirPublic);
    createPrivateTempFile(firstCacheFileInDirPrivate);
  }
  
  protected void refreshConf(Configuration conf) throws IOException {
    taskController.setConf(conf);
    UtilsForTests.setupTC(taskController, localDirAllocator,
        conf.getStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
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
    String userName = getJobOwnerName();
    subConf.set("user.name", userName);
    JobID jobid = new JobID("jt",1);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf);
    DistributedCache.addFileToClassPath(secondCacheFile, subConf, 
                                        FileSystem.get(subConf));
    TrackerDistributedCacheManager.determineTimestamps(subConf);
    TrackerDistributedCacheManager.determineCacheVisibilities(subConf);
    // ****** End of imitating JobClient code

    Path jobFile = new Path(TEST_ROOT_DIR, "job.xml");
    FileOutputStream os = new FileOutputStream(new File(jobFile.toString()));
    subConf.writeXml(os);
    os.close();

    // ****** Imitate TaskRunner code.
    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(conf, taskController);
    TaskDistributedCacheManager handle =
      manager.newTaskDistributedCacheManager(jobid, subConf);
    assertNull(null, DistributedCache.getLocalCacheFiles(subConf));
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());
    handle.setupCache(subConf, TaskTracker.getPublicDistributedCacheDir(), 
        TaskTracker.getPrivateDistributedCacheDir(userName));
    JobLocalizer.downloadPrivateCache(subConf);
    // DOESN'T ACTUALLY HAPPEN IN THE TaskRunner (THIS IS A TODO)
//    handle.setupPrivateCache(localDirAllocator, TaskTracker
//        .getPrivateDistributedCacheDir(userName));
//    // ****** End of imitating TaskRunner code

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
      super(conf, taskController);
    }

    @Override
    Path localizePublicCacheObject(Configuration conf, URI cache, 
                                   long confFileStamp,
                                   CacheStatus cacheStatus, 
                                   FileStatus fileStatus, 
                                   boolean isArchive) throws IOException {
      if (cache.equals(firstCacheFilePublic.toUri())) {
        throw new IOException("fake fail");
      }
      return super.localizePublicCacheObject(conf, cache, confFileStamp, 
                                             cacheStatus, fileStatus, 
                                             isArchive);
    }
  }

  public void testReferenceCount() throws IOException, LoginException,
      URISyntaxException, InterruptedException {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager = 
      new FakeTrackerDistributedCacheManager(conf);

    String userName = getJobOwnerName();
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());

    // Configures a job with a regular file
    Job job1 = new Job(conf);
    Configuration conf1 = job1.getConfiguration();
    conf1.set("user.name", userName);
    DistributedCache.addCacheFile(secondCacheFile.toUri(), conf1);
    
    TrackerDistributedCacheManager.determineTimestamps(conf1);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf1);

    // Task localizing for first job
    JobID jobId = new JobID("jt", 1);
    TaskDistributedCacheManager handle = manager
        .newTaskDistributedCacheManager(jobId, conf1);
    handle.setupCache(conf1, TaskTracker.getPublicDistributedCacheDir(), 
        TaskTracker.getPrivateDistributedCacheDir(userName));
    long[] sizes = JobLocalizer.downloadPrivateCache(conf1);
    if (sizes != null) {
      manager.setArchiveSizes(jobId, sizes);
    }
    handle.release();
    for (TaskDistributedCacheManager.CacheFile c : handle.getCacheFiles()) {
      assertEquals(0, manager.getReferenceCount(c.getStatus()));
      long filesize = FileUtil.getDU(new File(c.getStatus().localizedLoadPath.getParent().toString()));
      assertTrue("filesize is not greater than 0", filesize > 0);
      assertEquals(filesize, c.getStatus().size);
    }

    // Test specifying directories to go into distributed cache and make
    // their sizes are calculated properly.
    Job job2 = new Job(conf);
    Configuration conf2 = job2.getConfiguration();
    conf1.set("user.name", userName);
    DistributedCache.addCacheFile(firstCacheDirPublic.toUri(), conf2);
    DistributedCache.addCacheFile(firstCacheDirPrivate.toUri(), conf2);

    TrackerDistributedCacheManager.determineTimestamps(conf2);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf2);

    // Task localizing for second job
    JobID job2Id = new JobID("jt", 2);
    handle = manager.newTaskDistributedCacheManager(job2Id, conf2);
    handle.setupCache(conf2, TaskTracker.getPublicDistributedCacheDir(),
        TaskTracker.getPrivateDistributedCacheDir(userName));
    long[] sizes2 = JobLocalizer.downloadPrivateCache(conf2);
    for (int j=0; j > sizes2.length; j++) {
      LOG.info("size is: " + sizes2[j]);
    }
    if (sizes2 != null) {
      manager.setArchiveSizes(job2Id, sizes2);
    }
    handle.release();
    for (TaskDistributedCacheManager.CacheFile c : handle.getCacheFiles()) {
      assertEquals(0, manager.getReferenceCount(c.getStatus()));
      long filesize = FileUtil.getDU(new File(c.getStatus().localizedLoadPath.getParent().toString()));
      assertTrue("filesize is not greater than 0", filesize > 0);
      assertEquals(filesize, c.getStatus().size);
    }
    
    Path thirdCacheFile = new Path(TEST_ROOT_DIR, "thirdcachefile");
    createPrivateTempFile(thirdCacheFile);
    
    // Configures another job with three regular files.
    Job job3 = new Job(conf);
    Configuration conf3 = job3.getConfiguration();
    conf3.set("user.name", userName);
    // add a file that would get failed to localize
    DistributedCache.addCacheFile(firstCacheFilePublic.toUri(), conf3);
    // add a file that is already localized by different job
    DistributedCache.addCacheFile(secondCacheFile.toUri(), conf3);
    // add a file that is never localized
    DistributedCache.addCacheFile(thirdCacheFile.toUri(), conf3);
    
    TrackerDistributedCacheManager.determineTimestamps(conf3);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf3);

    // Task localizing for third job
    // localization for the "firstCacheFile" will fail.
    handle = manager.newTaskDistributedCacheManager(new JobID("jt", 3), conf3);
    Throwable th = null;
    try {
      handle.setupCache(conf3, TaskTracker.getPublicDistributedCacheDir(),
          TaskTracker.getPrivateDistributedCacheDir(userName));
      JobLocalizer.downloadPrivateCache(conf3);
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
        int refcount = manager.getReferenceCount(c.getStatus());
        LOG.info("checking refcount " + c.uri + " of " + refcount);
        assertEquals(0, refcount);
      } catch (NullPointerException ie) {
        th = ie;
        LOG.info("Exception getting reference count for " + c.uri, ie);
      }
    }
    assertNotNull(th);
    fs.delete(thirdCacheFile, false);
  }
  
  /**
   * Tests that localization of distributed cache file happens in the desired
   * directory
   * @throws IOException
   * @throws LoginException
   */
  public void testPublicPrivateCache() 
  throws IOException, LoginException, InterruptedException {
    if (!canRun()) {
      return;
    }
    checkLocalizedPath(true);
    checkLocalizedPath(false);
  }

  public void testSameNameFileArchiveCache() throws IOException,
      InterruptedException {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager = new TrackerDistributedCacheManager(
        conf, taskController);
    String userName = getJobOwnerName();
    File workDir = new File(TEST_ROOT_DIR, "workdir");
    Path cacheFile = new Path(TEST_ROOT_DIR, "fileArchiveCacheFile");

    createPublicTempFile(cacheFile);
    Configuration conf1 = new Configuration(conf);
    conf1.set("user.name", userName);

    DistributedCache.addCacheFile(cacheFile.toUri(), conf1);
    DistributedCache.addCacheArchive(cacheFile.toUri(), conf1);
    TrackerDistributedCacheManager.determineTimestamps(conf1);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf1);
    dumpState(conf1);

    TaskDistributedCacheManager handle = manager
        .newTaskDistributedCacheManager(new JobID("jt", 1), conf1);
    handle.setupCache(conf1, TaskTracker.getPublicDistributedCacheDir(),
        TaskTracker.getPrivateDistributedCacheDir(userName));

    TaskDistributedCacheManager.CacheFile cFile = handle.getCacheFiles().get(0);
    TaskDistributedCacheManager.CacheFile cArchive = handle.getCacheFiles()
        .get(1);

    String distCacheDir = TaskTracker.getPublicDistributedCacheDir();

    Path localizedPathForFile = manager.getLocalCache(cacheFile.toUri(), conf1,
        distCacheDir, fs.getFileStatus(cacheFile), false, cFile.timestamp,
        true, cFile);

    Path localizedPathForArchive = manager.getLocalCache(cacheFile.toUri(),
        conf1, distCacheDir, fs.getFileStatus(cacheFile), true,
        cArchive.timestamp, true, cArchive);
    assertNotSame("File and Archive resolve to the same path: "
        + localizedPathForFile + ". Should differ.", localizedPathForFile,
        localizedPathForArchive);
  }

  private void appendStringArray(StringBuilder buffer, String[] data) {
    if (data != null && data.length != 0) {
      buffer.append(data[0]);
      for(int i=1; i < data.length; i++) {
        buffer.append(',');
        buffer.append(data[i]);
      }
    }
  }

  private void appendBooleanArray(StringBuilder buffer, boolean[] data) {
    if (data != null && data.length != 0) {
      buffer.append(data[0]);
      for(int i=1; i < data.length; i++) {
        buffer.append(',');
        buffer.append(data[i]);
      }
    }
  }

  private void appendLongArray(StringBuilder buffer, long[] data) {
    if (data != null && data.length != 0) {
      buffer.append(data[0]);
      for(int i=1; i < data.length; i++) {
        buffer.append(',');
        buffer.append(data[i]);
      }
    }
  }

  private void appendUriArray(StringBuilder buffer, URI[] data) {
    if (data != null && data.length != 0) {
      buffer.append(data[0]);
      for(int i=1; i < data.length; i++) {
        buffer.append(',');
        buffer.append(data[i]);
      }
    }
  }

  private void dumpState(Configuration conf1) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append("\nFiles:");
    appendUriArray(buf, DistributedCache.getCacheFiles(conf1));
    buf.append("\nArchives:");
    appendUriArray(buf, DistributedCache.getCacheArchives(conf1));
    buf.append("\nFile Visible:");
    appendBooleanArray(buf, TrackerDistributedCacheManager.getFileVisibilities
                      (conf1));
    buf.append("\nArchive Visible:");
    appendBooleanArray(buf, TrackerDistributedCacheManager.getArchiveVisibilities
                      (conf1));
    buf.append("\nFile timestamps:");
    appendLongArray(buf, DistributedCache.getFileTimestamps(conf1));
    buf.append("\nArchive timestamps:");
    appendLongArray(buf, DistributedCache.getArchiveTimestamps(conf1));
    LOG.info("state = " + buf.toString());
  }
  
  private void checkLocalizedPath(boolean visibility) 
  throws IOException, LoginException, InterruptedException {
    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(conf, taskController);
    String userName = getJobOwnerName();
    File workDir = new File(TEST_ROOT_DIR, "workdir");
    Path cacheFile = new Path(TEST_ROOT_DIR, "fourthcachefile");
    if (visibility) {
      createPublicTempFile(cacheFile);
    } else {
      createPrivateTempFile(cacheFile);
    }

    Configuration conf1 = new Configuration(conf);
    conf1.set("user.name", userName);
    DistributedCache.addCacheFile(cacheFile.toUri(), conf1);
    TrackerDistributedCacheManager.determineTimestamps(conf1);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf1);
    dumpState(conf1);

    // Task localizing for job
    TaskDistributedCacheManager handle = manager
        .newTaskDistributedCacheManager(new JobID("jt", 1), conf1);
    handle.setupCache(conf1, TaskTracker.getPublicDistributedCacheDir(),
        TaskTracker.getPrivateDistributedCacheDir(userName));
    JobLocalizer.downloadPrivateCache(conf1);
    TaskDistributedCacheManager.CacheFile c = handle.getCacheFiles().get(0);
    String distCacheDir;
    if (visibility) {
      distCacheDir = TaskTracker.getPublicDistributedCacheDir(); 
    } else {
      distCacheDir = TaskTracker.getPrivateDistributedCacheDir(userName);
    }
    Path localizedPath =
      manager.getLocalCache(cacheFile.toUri(), conf1, distCacheDir,
                            fs.getFileStatus(cacheFile), false,
    		                c.timestamp, visibility, c);
    assertTrue("Cache file didn't get localized in the expected directory. " +
        "Expected localization to happen within " + 
        ROOT_MAPRED_LOCAL_DIR + "/" + distCacheDir +
        ", but was localized at " + 
        localizedPath, localizedPath.toString().contains(distCacheDir));
    if (visibility) {
      checkPublicFilePermissions(new Path[]{localizedPath});
    } else {
      checkFilePermissions(new Path[]{localizedPath});
    }
  }

  /**
   * Check proper permissions on the cache files
   * 
   * @param localCacheFiles
   * @throws IOException
   */
  protected void checkFilePermissions(Path[] localCacheFiles)
      throws IOException {
    // All the files should have executable permissions on them.
    for (Path p : localCacheFiles) {
      assertTrue("Cache file is not executable!", new File(p
          .toUri().getPath()).canExecute());
    }
  }

  /**
   * Check permissions on the public cache files
   * 
   * @param localCacheFiles
   * @throws IOException
   */
  private void checkPublicFilePermissions(Path[] localCacheFiles)
      throws IOException {
    checkPublicFilePermissions(fs, localCacheFiles);
  }

  /**
   * Verify the permissions for a file localized as a public distributed
   * cache file
   * @param fs The Local FileSystem used to get the permissions
   * @param localCacheFiles The list of files whose permissions should be 
   * verified.
   * @throws IOException
   */
  public static void checkPublicFilePermissions(FileSystem fs, 
      Path[] localCacheFiles) throws IOException {
    // All the files should have read and executable permissions for others
    for (Path p : localCacheFiles) {
      FsPermission perm = fs.getFileStatus(p).getPermission();
      assertTrue("cache file is not readable / executable by owner: perm="
          + perm.getUserAction(), perm.getUserAction()
          .implies(FsAction.READ_EXECUTE));
      assertTrue("cache file is not readable / executable by group: perm="
          + perm.getGroupAction(), perm.getGroupAction()
          .implies(FsAction.READ_EXECUTE));
      assertTrue("cache file is not readable / executable by others: perm="
          + perm.getOtherAction(), perm.getOtherAction()
          .implies(FsAction.READ_EXECUTE));
    }
  }
  
  /**
   * Verify the ownership for files localized as a public distributed cache
   * file.
   * @param fs The Local FileSystem used to get the ownership
   * @param localCacheFiles THe list of files whose ownership should be
   * verified
   * @param owner The owner of the files
   * @param group The group owner of the files.
   * @throws IOException
   */
  public static void checkPublicFileOwnership(FileSystem fs,
      Path[] localCacheFiles, String owner, String group)
      throws IOException {
    for (Path p: localCacheFiles) {
      assertEquals(owner, fs.getFileStatus(p).getOwner());
      assertEquals(group, fs.getFileStatus(p).getGroup());
    }
  }
  
  public static class MyTrackerDistributedCacheManager 
      extends TrackerDistributedCacheManager {

    public Throwable caught = null;
    public CountDownLatch done = new CountDownLatch(1);

    
    public MyTrackerDistributedCacheManager(Configuration conf,
        TaskController controller) throws IOException {
      super(conf, controller);
      this.baseDirManager = new TrackerDistributedCacheManager.BaseDirManager() {
        
        @Override
        void checkAndCleanup() throws IOException {
          throw new RuntimeException("This is a test!!!!");
        }
      };
      
      this.cleanupThread = new TestCleanupThread(conf);
    }
      
    class TestCleanupThread extends TrackerDistributedCacheManager.CleanupThread {
      
      public TestCleanupThread(Configuration conf) {
        super(conf);
      }

      @Override
      protected void exitTaskTracker(Throwable t) {
        caught = t;
        this.stopRunning();
        done.countDown();
      }        
    }
  }
  
  public void testRuntimeExceptionInCleanup() throws Exception {
    if(!canRun()) {
      return;
    }
    
    Configuration conf2 = new Configuration(conf);
    conf2.set("mapred.local.dir", ROOT_MAPRED_LOCAL_DIR.toString());
    conf2.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
    conf2.setLong("mapreduce.tasktracker.distributedcache.checkperiod", 0); // 0 ms (Don't sleep)
    
    refreshConf(conf2);
    MyTrackerDistributedCacheManager manager = 
        new MyTrackerDistributedCacheManager(conf2, taskController);
    manager.startCleanupThread();
    
    assertTrue(manager.done.await(200l, TimeUnit.MILLISECONDS));
    assertNotNull(manager.caught);
    assertTrue(manager.caught instanceof RuntimeException);
  }
  
  protected String getJobOwnerName() throws IOException {
    return UserGroupInformation.getLoginUser().getUserName();
  }

  public static final long CACHE_DELETE_PERIOD_MS = 100l;
  
  /** test delete cache */
  public void testLRUDeleteCache() throws Exception {
    if (!canRun()) {
      return;
    }
    // This test needs MRConfig.LOCAL_DIR to be single directory
    // instead of four, because it assumes that both 
    // firstcachefile and secondcachefile will be localized on same directory 
    // so that second localization triggers deleteCache.
    // If MRConfig.LOCAL_DIR is four directories, second localization might not 
    // trigger deleteCache, if it is localized in different directory.
    Configuration conf2 = new Configuration(conf);
    conf2.set("mapred.local.dir", ROOT_MAPRED_LOCAL_DIR.toString());
    //Make it larger then expected
    conf2.setLong("local.cache.size", 21 * 1024l);
    conf2.setLong("mapreduce.tasktracker.local.cache.numberdirectories", 3);
    //The goal is to get down to 15.75K and 2 dirs
    conf2.setFloat("mapreduce.tasktracker.cache.local.keep.pct", 0.75f); 
    conf2.setLong("mapreduce.tasktracker.distributedcache.checkperiod", CACHE_DELETE_PERIOD_MS);
    refreshConf(conf2);
    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(conf2, taskController);
    try {
      manager.startCleanupThread();
      FileSystem localfs = FileSystem.getLocal(conf2);
      String userName = getJobOwnerName();
      conf2.set("user.name", userName);

      //Here we are testing the LRU.  In this case we will add in 4 cache entries
      // 2 of them are 8k each and 2 of them are very small.  We want to verify
      // That they are deleted in LRU order.
      // So what we will do is add in the two large files first, 1 then 2, and
      // then one of the small ones 3.  We will then release them in opposite
      // order 3, 2, 1.
      //
      // Finally we will add in the last small file.  This last file should push
      // us over the 3 entry limit to trigger a cleanup.  So LRU order is 3, 2, 1
      // And we will only delete 2 entries so that should leave 1 un touched
      // but 3 and 2 deleted

      Path thirdCacheFile = new Path(TEST_ROOT_DIR, "thirdcachefile");
      Path fourthCacheFile = new Path(TEST_ROOT_DIR, "fourthcachefile");
      // Adding two more small files, so it triggers the number of sub directory
      // limit but does not trigger the file size limit.
      createTempFile(thirdCacheFile, 1);
      createTempFile(fourthCacheFile, 1);

      FileStatus stat = fs.getFileStatus(firstCacheFilePublic);
      CacheFile cfile1 = new CacheFile(firstCacheFilePublic.toUri(), 
          CacheFile.FileType.REGULAR, true, 
          stat.getModificationTime(),
          true); 

      Path firstLocalCache = manager.getLocalCache(firstCacheFilePublic.toUri(), conf2, 
          TaskTracker.getPrivateDistributedCacheDir(userName),
          fs.getFileStatus(firstCacheFilePublic), false,
          fs.getFileStatus(firstCacheFilePublic).getModificationTime(), true,
          cfile1);

      stat = fs.getFileStatus(secondCacheFilePublic);
      CacheFile cfile2 = new CacheFile(secondCacheFilePublic.toUri(), 
          CacheFile.FileType.REGULAR, true, 
          stat.getModificationTime(),
          true); 

      Path secondLocalCache = manager.getLocalCache(secondCacheFilePublic.toUri(), conf2, 
          TaskTracker.getPrivateDistributedCacheDir(userName),
          fs.getFileStatus(secondCacheFilePublic), false,
          fs.getFileStatus(secondCacheFilePublic).getModificationTime(), true,
          cfile2);

      stat = fs.getFileStatus(thirdCacheFile);
      CacheFile cfile3 = new CacheFile(thirdCacheFile.toUri(), 
          CacheFile.FileType.REGULAR, true, 
          stat.getModificationTime(),
          true); 

      Path thirdLocalCache = manager.getLocalCache(thirdCacheFile.toUri(), conf2, 
          TaskTracker.getPrivateDistributedCacheDir(userName),
          fs.getFileStatus(thirdCacheFile), false,
          fs.getFileStatus(thirdCacheFile).getModificationTime(), true,
          cfile3);

      manager.releaseCache(cfile3.getStatus());
      manager.releaseCache(cfile2.getStatus());
      manager.releaseCache(cfile1.getStatus());

      // Getting the fourth cache will make the number of sub directories becomes
      // 4 which is greater than 3. So the released cache will be deleted.
      stat = fs.getFileStatus(fourthCacheFile);
      CacheFile cfile4 = new CacheFile(fourthCacheFile.toUri(), 
          CacheFile.FileType.REGULAR, true, 
          stat.getModificationTime(),
          true); 

      Path fourthLocalCache = manager.getLocalCache(fourthCacheFile.toUri(), conf2, 
          TaskTracker.getPrivateDistributedCacheDir(userName),
          fs.getFileStatus(fourthCacheFile), false,
          fs.getFileStatus(fourthCacheFile).getModificationTime(), true,
          cfile4);

      checkCacheDeletion(localfs, secondLocalCache, "DistributedCache failed " +
          "deleting second cache LRU order");

      checkCacheDeletion(localfs, thirdLocalCache,
          "DistributedCache failed deleting third" +
      " cache LRU order.");

      checkCacheNOTDeletion(localfs, firstLocalCache, "DistributedCache failed " +
      "Deleted first cache LRU order.");

      checkCacheNOTDeletion(localfs, fourthCacheFile, "DistributedCache failed " +
      "Deleted fourth cache LRU order.");
      // Clean up the files created in this test
      new File(thirdCacheFile.toString()).delete();
      new File(fourthCacheFile.toString()).delete();
    } finally {
      manager.stopCleanupThread();
    }
  }

  
  /** test delete cache */
  public void testDeleteCache() throws Exception {
    if (!canRun()) {
      return;
    }
    // This test needs mapred.local.dir to be single directory
    // instead of four, because it assumes that both 
    // firstcachefile and secondcachefile will be localized on same directory 
    // so that second localization triggers deleteCache.
    // If mapred.local.dir is four directories, second localization might not 
    // trigger deleteCache, if it is localized in different directory.
    Configuration conf2 = new Configuration(conf);
    conf2.set("mapred.local.dir", ROOT_MAPRED_LOCAL_DIR.toString());
    conf2.setLong("local.cache.size", LOCAL_CACHE_LIMIT);
    conf2.setLong("mapreduce.tasktracker.distributedcache.checkperiod", CACHE_DELETE_PERIOD_MS);
    
    refreshConf(conf2);
    TrackerDistributedCacheManager manager = 
        new TrackerDistributedCacheManager(conf2, taskController);
    manager.startCleanupThread();
    try {
    FileSystem localfs = FileSystem.getLocal(conf2);
    long now = System.currentTimeMillis();
    String userName = getJobOwnerName();
    conf2.set("user.name", userName);

    // We first test the size limit
    FileStatus stat = fs.getFileStatus(firstCacheFilePublic);
    CacheFile cfile1 = new CacheFile(firstCacheFilePublic.toUri(), 
    		                         CacheFile.FileType.REGULAR, true, 
    		                         stat.getModificationTime(),
    		                         true); 
    Path firstLocalCache = manager.getLocalCache(firstCacheFilePublic.toUri(), conf2, 
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(firstCacheFilePublic), false,
        fs.getFileStatus(firstCacheFilePublic).getModificationTime(), true,
        cfile1);
    manager.releaseCache(cfile1.getStatus());
    //in above code,localized a file of size 4K and then release the cache 
    // which will cause the cache be deleted when the limit goes out. 
    // The below code localize another cache which's designed to
    //sweep away the first cache.
    stat = fs.getFileStatus(secondCacheFilePublic);
    CacheFile cfile2 = new CacheFile(secondCacheFilePublic.toUri(), 
    		                         CacheFile.FileType.REGULAR, true, 
    		                         stat.getModificationTime(),
    		                         true); 
    assertTrue("DistributedCache currently doesn't have cached file",
        localfs.exists(firstLocalCache));
    Path secondLocalCache = manager.getLocalCache(secondCacheFilePublic.toUri(), conf2, 
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(secondCacheFilePublic), false, 
        fs.getFileStatus(secondCacheFilePublic).getModificationTime(), true,
        cfile2);
    checkCacheDeletion(localfs, firstLocalCache,
        "DistributedCache failed deleting old" +
        " cache when the cache store is full");

    // find the root directory of distributed caches
    Path firstCursor = firstLocalCache;
    Path secondCursor = secondLocalCache;

    while (!firstCursor.equals(secondCursor)) {
      // Debug code, to see what these things look like
      System.err.println("cursors: " + firstCursor);
      System.err.println(" and " + secondCursor);

      firstCursor = firstCursor.getParent();
      secondCursor = secondCursor.getParent();
    }

    System.err.println("The final cursor is " + firstCursor);

    System.err.println("That directory ends up with "
                       + localfs.listStatus(firstCursor).length
                       + " subdirectories");
    Path cachesBase = firstCursor;

    assertFalse
      ("DistributedCache did not delete the gensym'ed distcache "
           + "directory names when it deleted the files they contained "
           + "because they collectively exceeded the size limit.",
       localfs.listStatus(cachesBase).length > 1);
    conf2.setLong("local.cache.size", LOCAL_CACHE_LIMIT * 10);
    conf2.setLong("mapreduce.tasktracker.local.cache.numberdirectories",
        LOCAL_CACHE_SUBDIR_LIMIT);
    manager.stopCleanupThread();
    
    manager = 
      new TrackerDistributedCacheManager(conf2, taskController);
    manager.startCleanupThread();
    
    // Now we test the number of sub directories limit
    // Create the temporary cache files to be used in the tests.
    Path thirdCacheFile = new Path(TEST_ROOT_DIR, "thirdcachefile");
    Path fourthCacheFile = new Path(TEST_ROOT_DIR, "fourthcachefile");
    // Adding two more small files, so it triggers the number of sub directory
    // limit but does not trigger the file size limit.
    createPrivateTempFile(thirdCacheFile);
    createPrivateTempFile(fourthCacheFile);
    DistributedCache.setCacheFiles(new URI[]{thirdCacheFile.toUri()}, conf2);
    TrackerDistributedCacheManager.determineCacheVisibilities(conf2);
    TrackerDistributedCacheManager.determineTimestamps(conf2);
    stat = fs.getFileStatus(thirdCacheFile);
    CacheFile cfile3 = new CacheFile(thirdCacheFile.toUri(), 
            CacheFile.FileType.REGULAR, false, 
            stat.getModificationTime(),
            true); 
    Path thirdLocalCache = manager.getLocalCache(thirdCacheFile.toUri(), conf2,
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(thirdCacheFile), false,
        fs.getFileStatus(thirdCacheFile).getModificationTime(), 
        false, cfile3);
    DistributedCache.setLocalFiles(conf2, thirdLocalCache.toString());
    JobLocalizer.downloadPrivateCache(conf2);
    // Release the third cache so that it can be deleted while sweeping
    manager.releaseCache(cfile3.getStatus());
    // Getting the fourth cache will make the number of sub directories becomes
    // 3 which is greater than 2. So the released cache will be deleted.
    stat = fs.getFileStatus(fourthCacheFile);
    CacheFile cfile4 = new CacheFile(fourthCacheFile.toUri(), 
            CacheFile.FileType.REGULAR, false, 
            stat.getModificationTime(),
            true); 
    assertTrue("DistributedCache currently doesn't have cached file",
        localfs.exists(thirdLocalCache));
    
    DistributedCache.setCacheFiles(new URI[]{fourthCacheFile.toUri()}, conf2);
    DistributedCache.setLocalFiles(conf2, thirdCacheFile.toUri().toString());
    TrackerDistributedCacheManager.determineCacheVisibilities(conf2);
    TrackerDistributedCacheManager.determineTimestamps(conf2);
    Path fourthLocalCache = manager.getLocalCache(fourthCacheFile.toUri(), conf2, 
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(fourthCacheFile), false, 
        fs.getFileStatus(fourthCacheFile).getModificationTime(), false, cfile4);
    checkCacheDeletion(localfs, thirdLocalCache,
        "DistributedCache failed deleting old" +
        " cache when the cache exceeds the number of sub directories limit.");

    assertFalse
      ("DistributedCache did not delete the gensym'ed distcache "
           + "directory names when it deleted the files they contained "
           + "because there were too many.",
       localfs.listStatus(cachesBase).length > LOCAL_CACHE_SUBDIR_LIMIT);

    // Clean up the files created in this test
    new File(thirdCacheFile.toString()).delete();
    new File(fourthCacheFile.toString()).delete();
    } finally {
      manager.stopCleanupThread();
    }
  }
  
  /**
   * Do a simple check to see if the file has NOT been deleted.
   */
  private void checkCacheNOTDeletion(FileSystem fs, Path cache, String msg)
  throws Exception {
    TimeUnit.MILLISECONDS.sleep(3 * CACHE_DELETE_PERIOD_MS);
    assertTrue(msg, fs.exists(cache));
  }
  
  /**
   * Periodically checks if a file is there, return if the file is no longer
   * there. Fails the test if a files is there for 30 seconds.
   */
  private void checkCacheDeletion(FileSystem fs, Path cache, String msg)
  throws Exception {
    // Check every 100ms to see if the cache is deleted
    boolean cacheExists = true;
    for (int i = 0; i < 300; i++) {
      if (!fs.exists(cache)) {
        cacheExists = false;
        break;
      }
      TimeUnit.MILLISECONDS.sleep(CACHE_DELETE_PERIOD_MS);
    }
    // If the cache is still there after 5 minutes, test fails.
    assertFalse(msg, cacheExists);
  }

  public void testFileSystemOtherThanDefault() throws Exception {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager =
      new TrackerDistributedCacheManager(conf, taskController);
    conf.set("fs.fakefile.impl", conf.get("fs.file.impl"));
    String userName = getJobOwnerName();
    conf.set("user.name", userName);
    Path fileToCache = new Path("fakefile:///"
        + firstCacheFile.toUri().getPath());
    CacheFile file = new CacheFile(fileToCache.toUri(), 
    		                       CacheFile.FileType.REGULAR, 
    		                       false, 0, false);
    Path result = manager.getLocalCache(fileToCache.toUri(), conf,
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(firstCacheFile), false,
        System.currentTimeMillis(),
        false, file);
    assertNotNull("DistributedCache cached file on non-default filesystem.",
        result);
  }

  static void createTempFile(Path p) throws IOException {
    createTempFile(p, TEST_FILE_SIZE);
  }
  
  static void createTempDir(Path p) throws IOException {
    File dir = new File(p.toString());
    dir.mkdirs();
    FileSystem.LOG.info("created temp directory: " + p);

  }

  static void createTempFile(Path p, int size) throws IOException {
    File f = new File(p.toString());
    FileOutputStream os = new FileOutputStream(f);
    byte[] toWrite = new byte[size];
    new Random().nextBytes(toWrite);
    os.write(toWrite);
    os.close();
    FileSystem.LOG.info("created: " + p + ", size=" + size);
  }
  
  static void createPublicTempFile(Path p) 
  throws IOException, InterruptedException {
    createTempFile(p);
    FileUtil.chmod(p.toString(), "0777",true);
  }
  
  static void createPrivateTempFile(Path p) 
  throws IOException, InterruptedException {
    createTempFile(p);
    FileUtil.chmod(p.toString(), "0770",true);
  }

  static void createPublicTempDir(Path p)
  throws IOException, InterruptedException {
    createTempDir(p);
    FileUtil.chmod(p.toString(), "0777",true);
  }

  static void createPrivateTempDir(Path p)
  throws IOException, InterruptedException {
    createTempDir(p);
    FileUtil.chmod(p.toString(), "0770",true);
  }

  @Override
  protected void tearDown() throws IOException {
    new File(firstCacheFile.toString()).delete();
    new File(secondCacheFile.toString()).delete();
    new File(firstCacheFilePublic.toString()).delete();
    new File(secondCacheFilePublic.toString()).delete();

    new File(firstCacheFileInDirPublic.toString()).delete();
    new File(firstCacheFileInDirPrivate.toString()).delete();
    new File(firstCacheDirPrivate.toString()).delete();
    new File(firstCacheDirPublic.toString()).delete();

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
    String userName = getJobOwnerName();

    TrackerDistributedCacheManager manager = 
      new TrackerDistributedCacheManager(myConf, taskController);
    // ****** Imitate JobClient code
    // Configures a task/job with both a regular file and a "classpath" file.
    Configuration subConf = new Configuration(myConf);
    subConf.set("user.name", userName);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf);
    TrackerDistributedCacheManager.determineTimestamps(subConf);
    TrackerDistributedCacheManager.determineCacheVisibilities(subConf);
    // ****** End of imitating JobClient code

    // ****** Imitate TaskRunner code.
    TaskDistributedCacheManager handle =
      manager.newTaskDistributedCacheManager(new JobID("jt", 1), subConf);
    assertNull(null, DistributedCache.getLocalCacheFiles(subConf));
    File workDir = new File(new Path(TEST_ROOT_DIR, "workdir").toString());
    handle.setupCache(subConf, TaskTracker.getPublicDistributedCacheDir(), 
        TaskTracker.getPrivateDistributedCacheDir(userName));
    //TODO this doesn't really happen in the TaskRunner
//    handle.setupPrivateCache(localDirAllocator, TaskTracker
//        .getPrivateDistributedCacheDir(userName));
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
      handle.setupCache(subConf, TaskTracker.getPublicDistributedCacheDir(),
          TaskTracker.getPrivateDistributedCacheDir(userName));
//      handle.setupPrivateCache(localDirAllocator, TaskTracker
//          .getPrivateDistributedCacheDir(userName));
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
    subConf2.set("user.name", userName);
    DistributedCache.addCacheFile(firstCacheFile.toUri(), subConf2);
    TrackerDistributedCacheManager.determineTimestamps(subConf2);
    TrackerDistributedCacheManager.determineCacheVisibilities(subConf2);
    
    handle =
      manager.newTaskDistributedCacheManager(new JobID("jt", 2), subConf2);
    handle.setupCache(subConf2, TaskTracker.getPublicDistributedCacheDir(), 
        TaskTracker.getPrivateDistributedCacheDir(userName));
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

  /**
   * Localize a file. After localization is complete, create a file, "myFile",
   * under the directory where the file is localized and ensure that it has
   * permissions different from what is set by default. Then, localize another
   * file. Verify that "myFile" has the right permissions.
   * @throws Exception
   */
  public void testCustomPermissions() throws Exception {
    if (!canRun()) {
      return;
    }
    String userName = getJobOwnerName();
    conf.set("user.name", userName);
    TrackerDistributedCacheManager manager = 
        new TrackerDistributedCacheManager(conf, taskController);
    FileSystem localfs = FileSystem.getLocal(conf);
    long now = System.currentTimeMillis();

    Path[] localCache = new Path[2];
    FileStatus stat = fs.getFileStatus(firstCacheFile);
    CacheFile file = new CacheFile(firstCacheFilePublic.toUri(), 
    		                       CacheFile.FileType.REGULAR, true, 
    		                       stat.getModificationTime(), false);
    localCache[0] = manager.getLocalCache(firstCacheFilePublic.toUri(), conf, 
        TaskTracker.getPrivateDistributedCacheDir(userName),
        fs.getFileStatus(firstCacheFilePublic), false,
        fs.getFileStatus(firstCacheFilePublic).getModificationTime(), true,
        file);
    FsPermission myPermission = new FsPermission((short)0600);
    Path myFile = new Path(localCache[0].getParent(), "myfile.txt");
    if (FileSystem.create(localfs, myFile, myPermission) == null) {
      throw new IOException("Could not create " + myFile);
    }
    try {
      stat = fs.getFileStatus(secondCacheFilePublic);
      file = new CacheFile(secondCacheFilePublic.toUri(),
    		               CacheFile.FileType.REGULAR,
    		               true, stat.getModificationTime(), false);
      localCache[1] = manager.getLocalCache(secondCacheFilePublic.toUri(), conf, 
          TaskTracker.getPrivateDistributedCacheDir(userName),
          fs.getFileStatus(secondCacheFilePublic), false, 
          fs.getFileStatus(secondCacheFilePublic).getModificationTime(), true,
          file);
      stat = localfs.getFileStatus(myFile);
      assertTrue(stat.getPermission().equals(myPermission));
      // validate permissions of localized files.
      checkFilePermissions(localCache);
    } finally {
      localfs.delete(myFile, false);
    }
  }

  public void testRemoveTaskDistributedCacheManager() throws Exception {
    if (!canRun()) {
      return;
    }
    TrackerDistributedCacheManager manager = new TrackerDistributedCacheManager(
        conf, taskController);
    JobID jobId = new JobID("jobtracker", 1);
    manager.newTaskDistributedCacheManager(jobId, conf);

    TaskDistributedCacheManager taskDistributedCacheManager = manager
        .getTaskDistributedCacheManager(jobId);
    assertNotNull(taskDistributedCacheManager);

    manager.removeTaskDistributedCacheManager(jobId);

    taskDistributedCacheManager = manager.getTaskDistributedCacheManager(jobId);
    assertNull(taskDistributedCacheManager);
  }

}
