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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = fs.makeQualified(new Path(dir.getAbsolutePath()));
    fs.delete(p, true);
  }

  @Test (timeout = 120000)
  public void testJobIDtoString() {
    JobId jid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jid.setAppId(ApplicationId.newInstance(0, 0));
    assertEquals("job_0_0000", MRApps.toString(jid));
  }

  @Test (timeout = 120000)
  public void testToJobID() {
    JobId jid = MRApps.toJobID("job_1_1");
    assertEquals(1, jid.getAppId().getClusterTimestamp());
    assertEquals(1, jid.getAppId().getId());
    assertEquals(1, jid.getId()); // tests against some proto.id and not a job.id field
  }

  @Test (timeout = 120000, expected=IllegalArgumentException.class)
  public void testJobIDShort() {
    MRApps.toJobID("job_0_0_0");
  }

  //TODO_get.set
  @Test (timeout = 120000)
  public void testTaskIDtoString() {
    TaskId tid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    tid.setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    tid.getJobId().setAppId(ApplicationId.newInstance(0, 0));
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

  @Test (timeout = 120000)
  public void testToTaskID() {
    TaskId tid = MRApps.toTaskID("task_1_2_r_3");
    assertEquals(1, tid.getJobId().getAppId().getClusterTimestamp());
    assertEquals(2, tid.getJobId().getAppId().getId());
    assertEquals(2, tid.getJobId().getId());
    assertEquals(TaskType.REDUCE, tid.getTaskType());
    assertEquals(3, tid.getId());

    tid = MRApps.toTaskID("task_1_2_m_3");
    assertEquals(TaskType.MAP, tid.getTaskType());
  }

  @Test(timeout = 120000, expected=IllegalArgumentException.class) 
  public void testTaskIDShort() {
    MRApps.toTaskID("task_0_0000_m");
  }

  @Test(timeout = 120000, expected=IllegalArgumentException.class) 
  public void testTaskIDBadType() {
    MRApps.toTaskID("task_0_0000_x_000000");
  }

  //TODO_get.set
  @Test (timeout = 120000)
  public void testTaskAttemptIDtoString() {
    TaskAttemptId taid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taid.setTaskId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class));
    taid.getTaskId().setTaskType(TaskType.MAP);
    taid.getTaskId().setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    taid.getTaskId().getJobId().setAppId(ApplicationId.newInstance(0, 0));
    assertEquals("attempt_0_0000_m_000000_0", MRApps.toString(taid));
  }

  @Test (timeout = 120000)
  public void testToTaskAttemptID() {
    TaskAttemptId taid = MRApps.toTaskAttemptID("attempt_0_1_m_2_3");
    assertEquals(0, taid.getTaskId().getJobId().getAppId().getClusterTimestamp());
    assertEquals(1, taid.getTaskId().getJobId().getAppId().getId());
    assertEquals(1, taid.getTaskId().getJobId().getId());
    assertEquals(2, taid.getTaskId().getId());
    assertEquals(3, taid.getId());
  }

  @Test(timeout = 120000, expected=IllegalArgumentException.class) 
  public void testTaskAttemptIDShort() {
    MRApps.toTaskAttemptID("attempt_0_0_0_m_0");
  }

  @Test (timeout = 120000)
  public void testGetJobFileWithUser() {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/my/path/to/staging");
    String jobFile = MRApps.getJobFile(conf, "dummy-user", 
        new JobID("dummy-job", 12345));
    assertNotNull("getJobFile results in null.", jobFile);
    assertEquals("jobFile with specified user is not as expected.",
        "/my/path/to/staging/dummy-user/.staging/job_dummy-job_12345/job.xml", jobFile);
  }

  @Test (timeout = 120000)
  public void testSetClasspath() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    Job job = Job.getInstance(conf);
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, job.getConfiguration());
    assertTrue(environment.get("CLASSPATH").startsWith(
      ApplicationConstants.Environment.PWD.$$()
          + ApplicationConstants.CLASS_PATH_SEPARATOR));
    String yarnAppClasspath = job.getConfiguration().get(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        StringUtils.join(",",
            YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH));
    if (yarnAppClasspath != null) {
      yarnAppClasspath =
          yarnAppClasspath.replaceAll(",\\s*",
            ApplicationConstants.CLASS_PATH_SEPARATOR).trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(yarnAppClasspath));
    String mrAppClasspath = 
        job.getConfiguration().get(
            MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
            MRJobConfig.DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH);
    if (mrAppClasspath != null) {
      mrAppClasspath =
          mrAppClasspath.replaceAll(",\\s*",
            ApplicationConstants.CLASS_PATH_SEPARATOR).trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(mrAppClasspath));
  }
  
  @Test (timeout = 120000)
  public void testSetClasspathWithArchives () throws IOException {
    File testTGZ = new File(testWorkDir, "test.tgz");
    FileOutputStream out = new FileOutputStream(testTGZ);
    out.write(0);
    out.close();
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    String testTGZQualifiedPath = FileSystem.getLocal(conf).makeQualified(new Path(
      testTGZ.getAbsolutePath())).toString();
    conf.set(MRJobConfig.CLASSPATH_ARCHIVES, testTGZQualifiedPath);
    conf.set(MRJobConfig.CACHE_ARCHIVES, testTGZQualifiedPath + "#testTGZ");
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, conf);
    assertTrue(environment.get("CLASSPATH").startsWith(
      ApplicationConstants.Environment.PWD.$$() + ApplicationConstants.CLASS_PATH_SEPARATOR));
    String confClasspath = job.getConfiguration().get(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        StringUtils.join(",",
            YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH));
    if (confClasspath != null) {
      confClasspath = confClasspath.replaceAll(",\\s*", ApplicationConstants.CLASS_PATH_SEPARATOR)
        .trim();
    }
    assertTrue(environment.get("CLASSPATH").contains(confClasspath));
    assertTrue(environment.get("CLASSPATH").contains("testTGZ"));
  }

 @Test (timeout = 120000)
 public void testSetClasspathWithUserPrecendence() {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    Map<String, String> env = new HashMap<String, String>();
    try {
      MRApps.setClasspath(env, conf);
    } catch (Exception e) {
      fail("Got exception while setting classpath");
    }
    String env_str = env.get("CLASSPATH");
    String expectedClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList(ApplicationConstants.Environment.PWD.$$(), "job.jar/*",
        "job.jar/classes/", "job.jar/lib/*",
        ApplicationConstants.Environment.PWD.$$() + "/*"));
    assertTrue("MAPREDUCE_JOB_USER_CLASSPATH_FIRST set, but not taking effect!",
      env_str.startsWith(expectedClasspath));
  }

  @Test (timeout = 120000)
  public void testSetClasspathWithNoUserPrecendence() {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
    Map<String, String> env = new HashMap<String, String>();
    try {
      MRApps.setClasspath(env, conf);
    } catch (Exception e) {
      fail("Got exception while setting classpath");
    }
    String env_str = env.get("CLASSPATH");
    String expectedClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList("job.jar/*", "job.jar/classes/", "job.jar/lib/*",
        ApplicationConstants.Environment.PWD.$$() + "/*"));
    assertTrue("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, and job.jar is not in"
      + " the classpath!", env_str.contains(expectedClasspath));
    assertFalse("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, but taking effect!",
      env_str.startsWith(expectedClasspath));
  }
  
  @Test (timeout = 120000)
  public void testSetClasspathWithJobClassloader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, true);
    Map<String, String> env = new HashMap<String, String>();
    MRApps.setClasspath(env, conf);
    String cp = env.get("CLASSPATH");
    String appCp = env.get("APP_CLASSPATH");
    assertFalse("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is in the"
      + " classpath!", cp.contains("jar" + ApplicationConstants.CLASS_PATH_SEPARATOR + "job"));
    assertFalse("MAPREDUCE_JOB_CLASSLOADER true, but PWD is in the classpath!",
      cp.contains("PWD"));
    String expectedAppClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList(ApplicationConstants.Environment.PWD.$$(), "job.jar/*",
        "job.jar/classes/", "job.jar/lib/*",
        ApplicationConstants.Environment.PWD.$$() + "/*"));
    assertEquals("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is not in the app"
      + " classpath!", expectedAppClasspath, appCp);
  }

  @Test (timeout = 3000000)
  public void testSetClasspathWithFramework() throws IOException {
    final String FRAMEWORK_NAME = "some-framework-name";
    final String FRAMEWORK_PATH = "some-framework-path#" + FRAMEWORK_NAME;
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    conf.set(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, FRAMEWORK_PATH);
    Map<String, String> env = new HashMap<String, String>();
    try {
      MRApps.setClasspath(env, conf);
      fail("Failed to catch framework path set without classpath change");
    } catch (IllegalArgumentException e) {
      assertTrue("Unexpected IllegalArgumentException",
          e.getMessage().contains("Could not locate MapReduce framework name '"
              + FRAMEWORK_NAME + "'"));
    }

    env.clear();
    final String FRAMEWORK_CLASSPATH = FRAMEWORK_NAME + "/*.jar";
    conf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, FRAMEWORK_CLASSPATH);
    MRApps.setClasspath(env, conf);
    final String stdClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList("job.jar/*", "job.jar/classes/", "job.jar/lib/*",
            ApplicationConstants.Environment.PWD.$$() + "/*"));
    String expectedClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList(ApplicationConstants.Environment.PWD.$$(),
            FRAMEWORK_CLASSPATH, stdClasspath));
    assertEquals("Incorrect classpath with framework and no user precedence",
        expectedClasspath, env.get("CLASSPATH"));

    env.clear();
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    MRApps.setClasspath(env, conf);
    expectedClasspath = StringUtils.join(ApplicationConstants.CLASS_PATH_SEPARATOR,
        Arrays.asList(ApplicationConstants.Environment.PWD.$$(),
            stdClasspath, FRAMEWORK_CLASSPATH));
    assertEquals("Incorrect classpath with framework and user precedence",
        expectedClasspath, env.get("CLASSPATH"));
  }

  @Test (timeout = 30000)
  public void testSetupDistributedCacheEmpty() throws IOException {
    Configuration conf = new Configuration();
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
    assertTrue("Empty Config did not produce an empty list of resources",
        localResources.isEmpty());
  }
  
  @SuppressWarnings("deprecation")
  @Test(timeout = 120000)
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

    assertEquals(1, localResources.size());
    LocalResource lr = localResources.get("something");
    //Archive wins
    assertNotNull(lr);
    assertEquals(10l, lr.getSize());
    assertEquals(10l, lr.getTimestamp());
    assertEquals(LocalResourceType.ARCHIVE, lr.getType());
  }
  
  @SuppressWarnings("deprecation")
  @Test(timeout = 120000)
  public void testSetupDistributedCacheConflictsFiles() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    
    URI mockUri = URI.create("mockfs://mock/");
    FileSystem mockFs = ((FilterFileSystem)FileSystem.get(mockUri, conf))
        .getRawFileSystem();
    
    URI file = new URI("mockfs://mock/tmp/something.zip#something");
    Path filePath = new Path(file);
    URI file2 = new URI("mockfs://mock/tmp/something.txt#something");
    Path file2Path = new Path(file2);
    
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

    assertEquals(1, localResources.size());
    LocalResource lr = localResources.get("something");
    //First one wins
    assertNotNull(lr);
    assertEquals(10l, lr.getSize());
    assertEquals(10l, lr.getTimestamp());
    assertEquals(LocalResourceType.FILE, lr.getType());
  }
  
  @SuppressWarnings("deprecation")
  @Test (timeout = 30000)
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

  @Test
  public void testLogSystemProperties() throws Exception {
    Configuration conf = new Configuration();
    // test no logging
    conf.set(MRJobConfig.MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG, " ");
    String value = MRApps.getSystemPropertiesToLog(conf);
    assertNull(value);

    // test logging of selected keys
    String classpath = "java.class.path";
    String os = "os.name";
    String version = "java.version";
    conf.set(MRJobConfig.MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG, classpath + ", " + os);
    value = MRApps.getSystemPropertiesToLog(conf);
    assertNotNull(value);
    assertTrue(value.contains(classpath));
    assertTrue(value.contains(os));
    assertFalse(value.contains(version));
  }

  @Test
  public void testTaskStateUI() {
    assertTrue(MRApps.TaskStateUI.PENDING.correspondsTo(TaskState.SCHEDULED));
    assertTrue(MRApps.TaskStateUI.COMPLETED.correspondsTo(TaskState.SUCCEEDED));
    assertTrue(MRApps.TaskStateUI.COMPLETED.correspondsTo(TaskState.FAILED));
    assertTrue(MRApps.TaskStateUI.COMPLETED.correspondsTo(TaskState.KILLED));
    assertTrue(MRApps.TaskStateUI.RUNNING.correspondsTo(TaskState.RUNNING));
  }


  private static final String[] SYS_CLASSES = new String[] {
      "/java/fake/Klass",
      "/javax/management/fake/Klass",
      "/org/apache/commons/logging/fake/Klass",
      "/org/apache/log4j/fake/Klass",
      "/org/apache/hadoop/fake/Klass"
  };

  private static final String[] DEFAULT_XMLS = new String[] {
        "core-default.xml",
      "mapred-default.xml",
        "hdfs-default.xml",
        "yarn-default.xml"
  };

  @Test
  public void testSystemClasses() {
    final List<String> systemClasses =
        Arrays.asList(StringUtils.getTrimmedStrings(
        ApplicationClassLoader.SYSTEM_CLASSES_DEFAULT));
    for (String defaultXml : DEFAULT_XMLS) {
      assertTrue(defaultXml + " must be system resource",
          ApplicationClassLoader.isSystemClass(defaultXml, systemClasses));
    }
    for (String klass : SYS_CLASSES) {
      assertTrue(klass + " must be system class",
          ApplicationClassLoader.isSystemClass(klass, systemClasses));
    }
    assertFalse("/fake/Klass must not be a system class",
        ApplicationClassLoader.isSystemClass("/fake/Klass", systemClasses));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWebappAddress() throws Exception {
    Configuration conf = new Configuration();
    conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, "19888");
    MRWebAppUtil.getApplicationWebURLOnJHSWithScheme(
         conf, ApplicationId.newInstance(0, 1));
  }
}
