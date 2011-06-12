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

package org.apache.hadoop.mapreduce.filecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.DefaultTaskController;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskController.InitializationContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.security.UserGroupInformation;

public class TestTrackerDistributedCacheManager extends TestCase {

  protected String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          TestTrackerDistributedCacheManager.class.getSimpleName())
          .getAbsolutePath();

  protected File ROOT_MAPRED_LOCAL_DIR;
  private static String TEST_CACHE_BASE_DIR;
  protected int numLocalDirs = 6;

  private static final int TEST_FILE_SIZE = 4 * 1024; // 4K
  private static final int LOCAL_CACHE_LIMIT = 5 * 1024; //5K
  protected Configuration conf;
  protected Path firstCacheFile;
  protected Path secondCacheFile;

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
    String []localDirs = new String[numLocalDirs];
    for (int i = 0; i < numLocalDirs; i++) {
      localDirs[i] = new File(ROOT_MAPRED_LOCAL_DIR, "0_" + i).getPath();
    }

    TEST_CACHE_BASE_DIR =
        new File(TEST_ROOT_DIR, "cachebasedir").getAbsolutePath();

    conf = new Configuration();
    conf.setLong(TTConfig.TT_LOCAL_CACHE_SIZE, LOCAL_CACHE_LIMIT);
    conf.setStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY, localDirs);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

    // Create the temporary cache files to be used in the tests.
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    createTempFile(firstCacheFile);
    createTempFile(secondCacheFile);
  }

  /**
   * This is the typical flow for using the DistributedCache classes.
   * 
   * @throws IOException
   * @throws LoginException
   */
  public void testManagerFlow() throws IOException, LoginException {

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
        dirStatuses.length == 1);
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
}
