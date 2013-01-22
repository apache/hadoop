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

package org.apache.hadoop.mapreduce.v2.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestMRApps {
  private static File testWorkDir = null;
  
  @BeforeClass
  public static void setupTestDirs() throws IOException {
    testWorkDir = new File("target", TestMRApps.class.getCanonicalName());
    delete(testWorkDir);
    testWorkDir.mkdirs();
    testWorkDir = testWorkDir.getAbsoluteFile();
  }
  
  @AfterClass
  public static void cleanupTestDirs() throws IOException {
    if (testWorkDir != null) {
      delete(testWorkDir);
    }
  }
  
  private static void delete(File dir) throws IOException {
    Path p = new Path("file://"+dir.getAbsolutePath());
    Configuration conf = new Configuration();
    FileSystem fs = p.getFileSystem(conf);
    fs.delete(p, true);
  }

  @Test public void testJobIDtoString() {
    JobId jid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jid.setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    assertEquals("job_0_0000", MRApps.toString(jid));
  }

  @Test public void testToJobID() {
    JobId jid = MRApps.toJobID("job_1_1");
    assertEquals(1, jid.getAppId().getClusterTimestamp());
    assertEquals(1, jid.getAppId().getId());
    assertEquals(1, jid.getId()); // tests against some proto.id and not a job.id field
  }

  @Test(expected=IllegalArgumentException.class) public void testJobIDShort() {
    MRApps.toJobID("job_0_0_0");
  }

  //TODO_get.set
  @Test public void testTaskIDtoString() {
    TaskId tid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    tid.setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    tid.getJobId().setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    tid.setTaskType(TaskType.MAP);
    TaskType type = tid.getTaskType();
    System.err.println(type);
    type = TaskType.REDUCE;
    System.err.println(type);
    System.err.println(tid.getTaskType());
    assertEquals("task_0_0000_m_000000", MRApps.toString(tid));
    tid.setTaskType(TaskType.REDUCE);
    assertEquals("task_0_0000_r_000000", MRApps.toString(tid));
  }

  @Test public void testToTaskID() {
    TaskId tid = MRApps.toTaskID("task_1_2_r_3");
    assertEquals(1, tid.getJobId().getAppId().getClusterTimestamp());
    assertEquals(2, tid.getJobId().getAppId().getId());
    assertEquals(2, tid.getJobId().getId());
    assertEquals(TaskType.REDUCE, tid.getTaskType());
    assertEquals(3, tid.getId());

    tid = MRApps.toTaskID("task_1_2_m_3");
    assertEquals(TaskType.MAP, tid.getTaskType());
  }

  @Test(expected=IllegalArgumentException.class) public void testTaskIDShort() {
    MRApps.toTaskID("task_0_0000_m");
  }

  @Test(expected=IllegalArgumentException.class) public void testTaskIDBadType() {
    MRApps.toTaskID("task_0_0000_x_000000");
  }

  //TODO_get.set
  @Test public void testTaskAttemptIDtoString() {
    TaskAttemptId taid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taid.setTaskId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class));
    taid.getTaskId().setTaskType(TaskType.MAP);
    taid.getTaskId().setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    taid.getTaskId().getJobId().setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    assertEquals("attempt_0_0000_m_000000_0", MRApps.toString(taid));
  }

  @Test public void testToTaskAttemptID() {
    TaskAttemptId taid = MRApps.toTaskAttemptID("attempt_0_1_m_2_3");
    assertEquals(0, taid.getTaskId().getJobId().getAppId().getClusterTimestamp());
    assertEquals(1, taid.getTaskId().getJobId().getAppId().getId());
    assertEquals(1, taid.getTaskId().getJobId().getId());
    assertEquals(2, taid.getTaskId().getId());
    assertEquals(3, taid.getId());
  }

  @Test(expected=IllegalArgumentException.class) public void testTaskAttemptIDShort() {
    MRApps.toTaskAttemptID("attempt_0_0_0_m_0");
  }

  @Test public void testGetJobFileWithUser() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/my/path/to/staging");
    String jobFile = MRApps.getJobFile(conf, "dummy-user", 
        new JobID("dummy-job", 12345));
    assertNotNull("getJobFile results in null.", jobFile);
    assertEquals("jobFile with specified user is not as expected.",
        "/my/path/to/staging/dummy-user/.staging/job_dummy-job_12345/job.xml", jobFile);
  }

  @Test public void testSetClasspath() throws IOException {
    Job job = Job.getInstance();
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, job.getConfiguration());
    assertTrue(environment.get("CLASSPATH").startsWith("$PWD:"));
    String yarnAppClasspath = 
        job.getConfiguration().get(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    if (yarnAppClasspath != null) {
      yarnAppClasspath = yarnAppClasspath.replaceAll(",\\s*", ":").trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(yarnAppClasspath));
    String mrAppClasspath = 
        job.getConfiguration().get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH);
    if (mrAppClasspath != null) {
      mrAppClasspath = mrAppClasspath.replaceAll(",\\s*", ":").trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(mrAppClasspath));
  }
  
  @Test public void testSetClasspathWithArchives () throws IOException {
    File testTGZ = new File(testWorkDir, "test.tgz");
    FileOutputStream out = new FileOutputStream(testTGZ);
    out.write(0);
    out.close();
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.CLASSPATH_ARCHIVES, "file://" 
        + testTGZ.getAbsolutePath());
    conf.set(MRJobConfig.CACHE_ARCHIVES, "file://"
        + testTGZ.getAbsolutePath() + "#testTGZ");
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, conf);
    assertTrue(environment.get("CLASSPATH").startsWith("$PWD:"));
    String confClasspath = job.getConfiguration().get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    if (confClasspath != null) {
      confClasspath = confClasspath.replaceAll(",\\s*", ":").trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(confClasspath));
    assertTrue(environment.get("CLASSPATH").contains("testTGZ"));
  }

 @Test public void testSetClasspathWithUserPrecendence() {
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    Map<String, String> env = new HashMap<String, String>();
    try {
      MRApps.setClasspath(env, conf);
    } catch (Exception e) {
      fail("Got exception while setting classpath");
    }
    String env_str = env.get("CLASSPATH");
    assertSame("MAPREDUCE_JOB_USER_CLASSPATH_FIRST set, but not taking effect!",
      env_str.indexOf("$PWD:job.jar/job.jar:job.jar/classes/:job.jar/lib/*:$PWD/*"), 0);
  }

  @Test public void testSetClasspathWithNoUserPrecendence() {
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
    Map<String, String> env = new HashMap<String, String>();
    try {
      MRApps.setClasspath(env, conf);
    } catch (Exception e) {
      fail("Got exception while setting classpath");
    }
    String env_str = env.get("CLASSPATH");
    int index = 
         env_str.indexOf("job.jar/job.jar:job.jar/classes/:job.jar/lib/*:$PWD/*");
    assertNotSame("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, and job.jar is not"
            + " in the classpath!", index, -1);
    assertNotSame("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, but taking effect!",
      index, 0);
  }
  
  @Test public void testSetClasspathWithJobClassloader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, true);
    Map<String, String> env = new HashMap<String, String>();
    MRApps.setClasspath(env, conf);
    String cp = env.get("CLASSPATH");
    String appCp = env.get("APP_CLASSPATH");
    assertSame("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is"
        + " in the classpath!", cp.indexOf("jar:job"), -1);
    assertSame("MAPREDUCE_JOB_CLASSLOADER true, but PWD is"
        + " in the classpath!", cp.indexOf("PWD"), -1);
    assertEquals("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is not"
        + " in the app classpath!",
        "$PWD:job.jar/job.jar:job.jar/classes/:job.jar/lib/*:$PWD/*", appCp);
  }
  
  @Test
  public void testSetupDistributedCacheEmpty() throws IOException {
    Configuration conf = new Configuration();
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
    assertTrue("Empty Config did not produce an empty list of resources",
        localResources.isEmpty());
  }
  
  @SuppressWarnings("deprecation")
  @Test(expected = InvalidJobConfException.class)
  public void testSetupDistributedCacheConflicts() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    
    URI mockUri = URI.create("mockfs://mock/");
    FileSystem mockFs = ((FilterFileSystem)FileSystem.get(mockUri, conf))
        .getRawFileSystem();
    
    URI archive = new URI("mockfs://mock/tmp/something.zip#something");
    Path archivePath = new Path(archive);
    URI file = new URI("mockfs://mock/tmp/something.txt#something");
    Path filePath = new Path(file);
    
    when(mockFs.resolvePath(archivePath)).thenReturn(archivePath);
    when(mockFs.resolvePath(filePath)).thenReturn(filePath);
    
    DistributedCache.addCacheArchive(archive, conf);
    conf.set(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS, "10");
    conf.set(MRJobConfig.CACHE_ARCHIVES_SIZES, "10");
    conf.set(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES, "true");
    DistributedCache.addCacheFile(file, conf);
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, "11");
    conf.set(MRJobConfig.CACHE_FILES_SIZES, "11");
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, "true");
    Map<String, LocalResource> localResources = 
      new HashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
  }
  
  @SuppressWarnings("deprecation")
  @Test(expected = InvalidJobConfException.class)
  public void testSetupDistributedCacheConflictsFiles() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    
    URI mockUri = URI.create("mockfs://mock/");
    FileSystem mockFs = ((FilterFileSystem)FileSystem.get(mockUri, conf))
        .getRawFileSystem();
    
    URI file = new URI("mockfs://mock/tmp/something.zip#something");
    Path filePath = new Path(file);
    URI file2 = new URI("mockfs://mock/tmp/something.txt#something");
    Path file2Path = new Path(file);
    
    when(mockFs.resolvePath(filePath)).thenReturn(filePath);
    when(mockFs.resolvePath(file2Path)).thenReturn(file2Path);
    
    DistributedCache.addCacheFile(file, conf);
    DistributedCache.addCacheFile(file2, conf);
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, "10,11");
    conf.set(MRJobConfig.CACHE_FILES_SIZES, "10,11");
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, "true,true");
    Map<String, LocalResource> localResources = 
      new HashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testSetupDistributedCache() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    
    URI mockUri = URI.create("mockfs://mock/");
    FileSystem mockFs = ((FilterFileSystem)FileSystem.get(mockUri, conf))
        .getRawFileSystem();
    
    URI archive = new URI("mockfs://mock/tmp/something.zip");
    Path archivePath = new Path(archive);
    URI file = new URI("mockfs://mock/tmp/something.txt#something");
    Path filePath = new Path(file);
    
    when(mockFs.resolvePath(archivePath)).thenReturn(archivePath);
    when(mockFs.resolvePath(filePath)).thenReturn(filePath);
    
    DistributedCache.addCacheArchive(archive, conf);
    conf.set(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS, "10");
    conf.set(MRJobConfig.CACHE_ARCHIVES_SIZES, "10");
    conf.set(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES, "true");
    DistributedCache.addCacheFile(file, conf);
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, "11");
    conf.set(MRJobConfig.CACHE_FILES_SIZES, "11");
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, "true");
    Map<String, LocalResource> localResources = 
      new HashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
    assertEquals(2, localResources.size());
    LocalResource lr = localResources.get("something.zip");
    assertNotNull(lr);
    assertEquals(10l, lr.getSize());
    assertEquals(10l, lr.getTimestamp());
    assertEquals(LocalResourceType.ARCHIVE, lr.getType());
    lr = localResources.get("something");
    assertNotNull(lr);
    assertEquals(11l, lr.getSize());
    assertEquals(11l, lr.getTimestamp());
    assertEquals(LocalResourceType.FILE, lr.getType());
  }
  
  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }
    public void initialize(URI name, Configuration conf) throws IOException {}
  }
  
}
