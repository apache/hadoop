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

package org.apache.hadoop.mapreduce.v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.FailingMapper;
import org.apache.hadoop.RandomTextWriterJob;
import org.apache.hadoop.RandomTextWriterJob.RandomInputFormat;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.SleepJob.SleepMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRJobs {

  private static final Log LOG = LogFactory.getLog(TestMRJobs.class);
  private static final EnumSet<RMAppState> TERMINAL_RM_APP_STATES =
      EnumSet.of(RMAppState.FINISHED, RMAppState.FAILED, RMAppState.KILLED);
  private static final int NUM_NODE_MGRS = 3;
  private static final String TEST_IO_SORT_MB = "11";
  private static final String TEST_GROUP_MAX = "200";

  private static final int DEFAULT_REDUCES = 2;
  protected int numSleepReducers = DEFAULT_REDUCES;

  protected static MiniMRYarnCluster mrCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  private static FileSystem remoteFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR = new Path("target",
      TestMRJobs.class.getName() + "-tmpDir").makeQualified(localFs);
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
  private static final String OUTPUT_ROOT_DIR = "/tmp/" +
    TestMRJobs.class.getSimpleName();

  @BeforeClass
  public static void setup() throws IOException {
    try {
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestMRJobs.class.getName(),
          NUM_NODE_MGRS);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      conf.setInt("yarn.cluster.max-application-priority", 10);
      mrCluster.init(conf);
      mrCluster.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @After
  public void resetInit() {
    numSleepReducers = DEFAULT_REDUCES;
  }

  @Test (timeout = 300000)
  public void testSleepJob() throws Exception {
    testSleepJobInternal(false);
  }

  @Test (timeout = 300000)
  public void testSleepJobWithRemoteJar() throws Exception {
    testSleepJobInternal(true);
  }

  private void testSleepJobInternal(boolean useRemoteJar) throws Exception {
    LOG.info("\n\n\nStarting testSleepJob: useRemoteJar=" + useRemoteJar);

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrCluster.getConfig());
    // set master address to local to test that local mode applied iff framework == local
    sleepConf.set(MRConfig.MASTER_ADDRESS, "local");	
    
    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(sleepConf);
   
    // job with 3 maps (10s) and numReduces reduces (5s), 1 "record" each:
    Job job = sleepJob.createJob(3, numSleepReducers, 10000, 1, 5000, 1);

    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    if (useRemoteJar) {
      final Path localJar = new Path(
          ClassUtil.findContainingJar(SleepJob.class));
      ConfigUtil.addLink(job.getConfiguration(), "/jobjars",
          localFs.makeQualified(localJar.getParent()).toUri());
      job.setJar("viewfs:///jobjars/" + localJar.getName());
    } else {
      job.setJarByClass(SleepJob.class);
    }
    job.setMaxMapAttempts(1); // speed up failures
    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
    verifySleepJobCounters(job);
    verifyTaskProgress(job);
    
    // TODO later:  add explicit "isUber()" checks of some sort (extend
    // JobStatus?)--compare against MRJobConfig.JOB_UBERTASK_ENABLE value
  }

  @Test(timeout = 3000000)
  public void testJobWithChangePriority() throws Exception {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    Configuration sleepConf = new Configuration(mrCluster.getConfig());
    // set master address to local to test that local mode applied if framework
    // equals local
    sleepConf.set(MRConfig.MASTER_ADDRESS, "local");
    sleepConf
        .setInt("yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms", 5);

    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(sleepConf);
    Job job = sleepJob.createJob(1, 1, 1000, 20, 50, 1);

    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setJarByClass(SleepJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.submit();

    // Set the priority to HIGH
    job.setPriority(JobPriority.HIGH);
    waitForPriorityToUpdate(job, JobPriority.HIGH);
    // Verify the priority from job itself
    Assert.assertEquals(job.getPriority(), JobPriority.HIGH);

    // Change priority to NORMAL (3) with new api
    job.setPriorityAsInteger(3); // Verify the priority from job itself
    waitForPriorityToUpdate(job, JobPriority.NORMAL);
    Assert.assertEquals(job.getPriority(), JobPriority.NORMAL);

    // Change priority to a high integer value with new api
    job.setPriorityAsInteger(89); // Verify the priority from job itself
    waitForPriorityToUpdate(job, JobPriority.UNDEFINED_PRIORITY);
    Assert.assertEquals(job.getPriority(), JobPriority.UNDEFINED_PRIORITY);

    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
  }

  private void waitForPriorityToUpdate(Job job, JobPriority expectedStatus)
      throws IOException, InterruptedException {
    // Max wait time to get the priority update can be kept as 20sec (100 *
    // 100ms)
    int waitCnt = 200;
    while (waitCnt-- > 0) {
      if (job.getPriority().equals(expectedStatus)) {
        // Stop waiting as priority is updated.
        break;
      } else {
        Thread.sleep(100);
      }
    }
  }

  @Test(timeout = 300000)
  public void testConfVerificationWithClassloader() throws Exception {
    testConfVerification(true, false, false, false);
  }

  @Test(timeout = 300000)
  public void testConfVerificationWithClassloaderCustomClasses()
      throws Exception {
    testConfVerification(true, true, false, false);
  }

  @Test(timeout = 300000)
  public void testConfVerificationWithOutClassloader() throws Exception {
    testConfVerification(false, false, false, false);
  }

  @Test(timeout = 300000)
  public void testConfVerificationWithJobClient() throws Exception {
    testConfVerification(false, false, true, false);
  }

  @Test(timeout = 300000)
  public void testConfVerificationWithJobClientLocal() throws Exception {
    testConfVerification(false, false, true, true);
  }

  private void testConfVerification(boolean useJobClassLoader,
      boolean useCustomClasses, boolean useJobClientForMonitring,
      boolean useLocal) throws Exception {
    LOG.info("\n\n\nStarting testConfVerification()"
        + " jobClassloader=" + useJobClassLoader
        + " customClasses=" + useCustomClasses
        + " jobClient=" + useJobClientForMonitring
        + " localMode=" + useLocal);

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }
    final Configuration clusterConfig;
    if (useLocal) {
      clusterConfig = new Configuration();
      conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    } else {
      clusterConfig = mrCluster.getConfig();
    }
    final JobClient jc = new JobClient(clusterConfig);
    final Configuration sleepConf = new Configuration(clusterConfig);
    // set master address to local to test that local mode applied iff framework == local
    sleepConf.set(MRConfig.MASTER_ADDRESS, "local");
    sleepConf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER,
        useJobClassLoader);
    if (useCustomClasses) {
      // to test AM loading user classes such as output format class, we want
      // to blacklist them from the system classes (they need to be prepended
      // as the first match wins)
      String systemClasses = ApplicationClassLoader.SYSTEM_CLASSES_DEFAULT;
      // exclude the custom classes from system classes
      systemClasses = "-" + CustomOutputFormat.class.getName() + ",-" +
          CustomSpeculator.class.getName() + "," +
          systemClasses;
      sleepConf.set(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES,
          systemClasses);
    }
    sleepConf.set(MRJobConfig.IO_SORT_MB, TEST_IO_SORT_MB);
    sleepConf.set(MRJobConfig.MR_AM_LOG_LEVEL, Level.ALL.toString());
    sleepConf.set(MRJobConfig.MAP_LOG_LEVEL, Level.ALL.toString());
    sleepConf.set(MRJobConfig.REDUCE_LOG_LEVEL, Level.ALL.toString());
    sleepConf.set(MRJobConfig.MAP_JAVA_OPTS, "-verbose:class");
    sleepConf.set(MRJobConfig.COUNTER_GROUPS_MAX_KEY, TEST_GROUP_MAX);
    final SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(sleepConf);
    final Job job = sleepJob.createJob(1, 1, 10, 1, 10, 1);
    job.setMapperClass(ConfVerificationMapper.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setJarByClass(SleepJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    if (useCustomClasses) {
      // set custom output format class and speculator class
      job.setOutputFormatClass(CustomOutputFormat.class);
      final Configuration jobConf = job.getConfiguration();
      jobConf.setClass(MRJobConfig.MR_AM_JOB_SPECULATOR, CustomSpeculator.class,
          Speculator.class);
      // speculation needs to be enabled for the speculator to be loaded
      jobConf.setBoolean(MRJobConfig.MAP_SPECULATIVE, true);
    }
    job.submit();
    final boolean succeeded;
    if (useJobClientForMonitring && !useLocal) {
      // We can't use getJobID in useLocal case because JobClient and Job
      // point to different instances of LocalJobRunner
      //
      final JobID mapredJobID = JobID.downgrade(job.getJobID());
      RunningJob runningJob = null;
      do {
        Thread.sleep(10);
        runningJob = jc.getJob(mapredJobID);
      } while (runningJob == null);
      Assert.assertEquals("Unexpected RunningJob's "
          + MRJobConfig.COUNTER_GROUPS_MAX_KEY,
          TEST_GROUP_MAX, runningJob.getConfiguration()
              .get(MRJobConfig.COUNTER_GROUPS_MAX_KEY));
      runningJob.waitForCompletion();
      succeeded = runningJob.isSuccessful();
    } else {
      succeeded = job.waitForCompletion(true);
    }
    Assert.assertTrue("Job status: " + job.getStatus().getFailureInfo(),
        succeeded);
  }

  public static class CustomOutputFormat<K,V> extends NullOutputFormat<K,V> {
    public CustomOutputFormat() {
      verifyClassLoader(getClass());
    }

    /**
     * Verifies that the class was loaded by the job classloader if it is in the
     * context of the MRAppMaster, and if not throws an exception to fail the
     * job.
     */
    private void verifyClassLoader(Class<?> cls) {
      // to detect that it is instantiated in the context of the MRAppMaster, we
      // inspect the stack trace and determine a caller is MRAppMaster
      for (StackTraceElement e: new Throwable().getStackTrace()) {
        if (e.getClassName().equals(MRAppMaster.class.getName()) &&
            !(cls.getClassLoader() instanceof ApplicationClassLoader)) {
          throw new ExceptionInInitializerError("incorrect classloader used");
        }
      }
    }
  }

  public static class CustomSpeculator extends DefaultSpeculator {
    public CustomSpeculator(Configuration conf, AppContext context) {
      super(conf, context);
      verifyClassLoader(getClass());
    }

    /**
     * Verifies that the class was loaded by the job classloader if it is in the
     * context of the MRAppMaster, and if not throws an exception to fail the
     * job.
     */
    private void verifyClassLoader(Class<?> cls) {
      // to detect that it is instantiated in the context of the MRAppMaster, we
      // inspect the stack trace and determine a caller is MRAppMaster
      for (StackTraceElement e: new Throwable().getStackTrace()) {
        if (e.getClassName().equals(MRAppMaster.class.getName()) &&
            !(cls.getClassLoader() instanceof ApplicationClassLoader)) {
          throw new ExceptionInInitializerError("incorrect classloader used");
        }
      }
    }
  }

  protected void verifySleepJobCounters(Job job) throws InterruptedException,
      IOException {
    Counters counters = job.getCounters();
    Assert.assertEquals(3, counters.findCounter(JobCounter.OTHER_LOCAL_MAPS)
        .getValue());
    Assert.assertEquals(3, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
        .getValue());
    Assert.assertEquals(numSleepReducers,
        counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES).getValue());
  }
  
  protected void verifyTaskProgress(Job job) throws InterruptedException,
      IOException {
    for (TaskReport taskReport : job.getTaskReports(TaskType.MAP)) {
      Assert.assertTrue(0.9999f < taskReport.getProgress()
          && 1.0001f > taskReport.getProgress());
    }
    for (TaskReport taskReport : job.getTaskReports(TaskType.REDUCE)) {
      Assert.assertTrue(0.9999f < taskReport.getProgress()
          && 1.0001f > taskReport.getProgress());
    }
  }

  @Test (timeout = 60000)
  public void testRandomWriter() throws IOException, InterruptedException,
      ClassNotFoundException {
    
    LOG.info("\n\n\nStarting testRandomWriter().");
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    RandomTextWriterJob randomWriterJob = new RandomTextWriterJob();
    mrCluster.getConfig().set(RandomTextWriterJob.TOTAL_BYTES, "3072");
    mrCluster.getConfig().set(RandomTextWriterJob.BYTES_PER_MAP, "1024");
    Job job = randomWriterJob.createJob(mrCluster.getConfig());
    Path outputDir = new Path(OUTPUT_ROOT_DIR, "random-output");
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setSpeculativeExecution(false);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setJarByClass(RandomTextWriterJob.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
    
    // Make sure there are three files in the output-dir
    
    RemoteIterator<FileStatus> iterator =
        FileContext.getFileContext(mrCluster.getConfig()).listStatus(
            outputDir);
    int count = 0;
    while (iterator.hasNext()) {
      FileStatus file = iterator.next();
      if (!file.getPath().getName()
          .equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)) {
        count++;
      }
    }
    Assert.assertEquals("Number of part files is wrong!", 3, count);
    verifyRandomWriterCounters(job);

    // TODO later:  add explicit "isUber()" checks of some sort
  }
  
  protected void verifyRandomWriterCounters(Job job)
      throws InterruptedException, IOException {
    Counters counters = job.getCounters();
    Assert.assertEquals(3, counters.findCounter(JobCounter.OTHER_LOCAL_MAPS)
        .getValue());
    Assert.assertEquals(3, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
        .getValue());
  }

  @Test (timeout = 60000)
  public void testFailingMapper() throws IOException, InterruptedException,
      ClassNotFoundException {

    LOG.info("\n\n\nStarting testFailingMapper().");

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Job job = runFailingMapperJob();

    TaskID taskID = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptID aId = new TaskAttemptID(taskID, 0);
    System.out.println("Diagnostics for " + aId + " :");
    for (String diag : job.getTaskDiagnostics(aId)) {
      System.out.println(diag);
    }
    aId = new TaskAttemptID(taskID, 1);
    System.out.println("Diagnostics for " + aId + " :");
    for (String diag : job.getTaskDiagnostics(aId)) {
      System.out.println(diag);
    }
    
    TaskCompletionEvent[] events = job.getTaskCompletionEvents(0, 2);
    Assert.assertEquals(TaskCompletionEvent.Status.FAILED, 
        events[0].getStatus());
    Assert.assertEquals(TaskCompletionEvent.Status.TIPFAILED, 
        events[1].getStatus());
    Assert.assertEquals(JobStatus.State.FAILED, job.getJobState());
    verifyFailingMapperCounters(job);

    // TODO later:  add explicit "isUber()" checks of some sort
  }
  
  protected void verifyFailingMapperCounters(Job job)
      throws InterruptedException, IOException {
    Counters counters = job.getCounters();
    Assert.assertEquals(2, counters.findCounter(JobCounter.OTHER_LOCAL_MAPS)
        .getValue());
    Assert.assertEquals(2, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
        .getValue());
    Assert.assertEquals(2, counters.findCounter(JobCounter.NUM_FAILED_MAPS)
        .getValue());
    Assert
        .assertTrue(counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS) != null
            && counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue() != 0);
  }

  protected Job runFailingMapperJob()
  throws IOException, InterruptedException, ClassNotFoundException {
    Configuration myConf = new Configuration(mrCluster.getConfig());
    myConf.setInt(MRJobConfig.NUM_MAPS, 1);
    myConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 2); //reduce the number of attempts

    Job job = Job.getInstance(myConf);

    job.setJarByClass(FailingMapper.class);
    job.setJobName("failmapper");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(RandomInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(FailingMapper.class);
    job.setNumReduceTasks(0);
    
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_ROOT_DIR,
      "failmapper-output"));
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    boolean succeeded = job.waitForCompletion(true);
    Assert.assertFalse(succeeded);
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
    return job;
  }

  //@Test (timeout = 60000)
  public void testSleepJobWithSecurityOn() throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("\n\n\nStarting testSleepJobWithSecurityOn().");

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      return;
    }

    mrCluster.getConfig().set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    mrCluster.getConfig().set(YarnConfiguration.RM_KEYTAB, "/etc/krb5.keytab");
    mrCluster.getConfig().set(YarnConfiguration.NM_KEYTAB, "/etc/krb5.keytab");
    mrCluster.getConfig().set(YarnConfiguration.RM_PRINCIPAL,
        "rm/sightbusy-lx@LOCALHOST");
    mrCluster.getConfig().set(YarnConfiguration.NM_PRINCIPAL,
        "nm/sightbusy-lx@LOCALHOST");
    UserGroupInformation.setConfiguration(mrCluster.getConfig());

    // Keep it in here instead of after RM/NM as multiple user logins happen in
    // the same JVM.
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    LOG.info("User name is " + user.getUserName());
    for (Token<? extends TokenIdentifier> str : user.getTokens()) {
      LOG.info("Token is " + str.encodeToUrlString());
    }
    user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {  
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(mrCluster.getConfig());
        Job job = sleepJob.createJob(3, 0, 10000, 1, 0, 0);
        // //Job with reduces
        // Job job = sleepJob.createJob(3, 2, 10000, 1, 10000, 1);
        job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
        job.submit();
        String trackingUrl = job.getTrackingURL();
        String jobId = job.getJobID().toString();
        job.waitForCompletion(true);
        Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
        Assert.assertTrue("Tracking URL was " + trackingUrl +
                          " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
        return null;
      }
    });

    // TODO later:  add explicit "isUber()" checks of some sort
  }

  @Test(timeout = 120000)
  public void testContainerRollingLog() throws IOException,
      InterruptedException, ClassNotFoundException {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    final SleepJob sleepJob = new SleepJob();
    final JobConf sleepConf = new JobConf(mrCluster.getConfig());
    sleepConf.set(MRJobConfig.MAP_LOG_LEVEL, Level.ALL.toString());
    final long userLogKb = 4;
    sleepConf.setLong(MRJobConfig.TASK_USERLOG_LIMIT, userLogKb);
    sleepConf.setInt(MRJobConfig.TASK_LOG_BACKUPS, 3);
    sleepConf.set(MRJobConfig.MR_AM_LOG_LEVEL, Level.ALL.toString());
    final long amLogKb = 7;
    sleepConf.setLong(MRJobConfig.MR_AM_LOG_KB, amLogKb);
    sleepConf.setInt(MRJobConfig.MR_AM_LOG_BACKUPS, 7);
    sleepJob.setConf(sleepConf);

    final Job job = sleepJob.createJob(1, 0, 1L, 100, 0L, 0);
    job.setJarByClass(SleepJob.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.waitForCompletion(true);
    final JobId jobId = TypeConverter.toYarn(job.getJobID());
    final ApplicationId appID = jobId.getAppId();
    int pollElapsed = 0;
    while (true) {
      Thread.sleep(1000);
      pollElapsed += 1000;
      if (TERMINAL_RM_APP_STATES.contains(
        mrCluster.getResourceManager().getRMContext().getRMApps().get(appID)
            .getState())) {
        break;
      }
      if (pollElapsed >= 60000) {
        LOG.warn("application did not reach terminal state within 60 seconds");
        break;
      }
    }
    Assert.assertEquals(RMAppState.FINISHED, mrCluster.getResourceManager()
        .getRMContext().getRMApps().get(appID).getState());

    // Job finished, verify logs
    //

    final String appIdStr = appID.toString();
    final String appIdSuffix = appIdStr.substring("application_".length(),
        appIdStr.length());
    final String containerGlob = "container_" + appIdSuffix + "_*_*";
    final String syslogGlob = appIdStr
        + Path.SEPARATOR + containerGlob
        + Path.SEPARATOR + TaskLog.LogName.SYSLOG;
    int numAppMasters = 0;
    int numMapTasks = 0;

    for (int i = 0; i < NUM_NODE_MGRS; i++) {
      final Configuration nmConf = mrCluster.getNodeManager(i).getConfig();
      for (String logDir :
               nmConf.getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)) {
        final Path absSyslogGlob =
            new Path(logDir + Path.SEPARATOR + syslogGlob);
        LOG.info("Checking for glob: " + absSyslogGlob);
        final FileStatus[] syslogs = localFs.globStatus(absSyslogGlob);
        for (FileStatus slog : syslogs) {
          boolean foundAppMaster = job.isUber();
          final Path containerPathComponent = slog.getPath().getParent();
          if (!foundAppMaster) {
            final ContainerId cid = ContainerId.fromString(
                containerPathComponent.getName());
            foundAppMaster =
                ((cid.getContainerId() & ContainerId.CONTAINER_ID_BITMASK)== 1);
          }

          final FileStatus[] sysSiblings = localFs.globStatus(new Path(
              containerPathComponent, TaskLog.LogName.SYSLOG + "*"));
          // sort to ensure for i > 0 sysSiblings[i] == "syslog.i"
          Arrays.sort(sysSiblings);

          if (foundAppMaster) {
            numAppMasters++;
          } else {
            numMapTasks++;
          }

          if (foundAppMaster) {
            Assert.assertSame("Unexpected number of AM sylog* files",
                sleepConf.getInt(MRJobConfig.MR_AM_LOG_BACKUPS, 0) + 1,
                sysSiblings.length);
            Assert.assertTrue("AM syslog.1 length kb should be >= " + amLogKb,
                sysSiblings[1].getLen() >= amLogKb * 1024);
          } else {
            Assert.assertSame("Unexpected number of MR task sylog* files",
                sleepConf.getInt(MRJobConfig.TASK_LOG_BACKUPS, 0) + 1,
                sysSiblings.length);
            Assert.assertTrue("MR syslog.1 length kb should be >= " + userLogKb,
                sysSiblings[1].getLen() >= userLogKb * 1024);
          }
        }
      }
    }
    // Make sure we checked non-empty set
    //
    Assert.assertEquals("No AppMaster log found!", 1, numAppMasters);
    if (sleepConf.getBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false)) {
      Assert.assertEquals("MapTask log with uber found!", 0, numMapTasks);
    } else {
      Assert.assertEquals("No MapTask log found!", 1, numMapTasks);
    }
  }

  public static class DistributedCacheChecker extends
      Mapper<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path[] localFiles = context.getLocalCacheFiles();
      URI[] files = context.getCacheFiles();
      Path[] localArchives = context.getLocalCacheArchives();
      URI[] archives = context.getCacheArchives();

      // Check that 4 (2 + appjar + DistrubutedCacheChecker jar) files 
      // and 2 archives are present
      Assert.assertEquals(4, localFiles.length);
      Assert.assertEquals(4, files.length);
      Assert.assertEquals(2, localArchives.length);
      Assert.assertEquals(2, archives.length);

      // Check lengths of the files
      Map<String, Path> filesMap = pathsToMap(localFiles);
      Assert.assertTrue(filesMap.containsKey("distributed.first.symlink"));
      Assert.assertEquals(1, localFs.getFileStatus(
        filesMap.get("distributed.first.symlink")).getLen());
      Assert.assertTrue(filesMap.containsKey("distributed.second.jar"));
      Assert.assertTrue(localFs.getFileStatus(
        filesMap.get("distributed.second.jar")).getLen() > 1);

      // Check extraction of the archive
      Map<String, Path> archivesMap = pathsToMap(localArchives);
      Assert.assertTrue(archivesMap.containsKey("distributed.third.jar"));
      Assert.assertTrue(localFs.exists(new Path(
        archivesMap.get("distributed.third.jar"), "distributed.jar.inside3")));
      Assert.assertTrue(archivesMap.containsKey("distributed.fourth.jar"));
      Assert.assertTrue(localFs.exists(new Path(
        archivesMap.get("distributed.fourth.jar"), "distributed.jar.inside4")));

      // Check the class loaders
      LOG.info("Java Classpath: " + System.getProperty("java.class.path"));
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      // Both the file and the archive should have been added to classpath, so
      // both should be reachable via the class loader.
      Assert.assertNotNull(cl.getResource("distributed.jar.inside2"));
      Assert.assertNotNull(cl.getResource("distributed.jar.inside3"));
      Assert.assertNotNull(cl.getResource("distributed.jar.inside4"));
      // The Job Jar should have been extracted to a folder named "job.jar" and
      // added to the classpath; the two jar files in the lib folder in the Job
      // Jar should have also been added to the classpath
      Assert.assertNotNull(cl.getResource("job.jar/"));
      Assert.assertNotNull(cl.getResource("job.jar/lib/lib1.jar"));
      Assert.assertNotNull(cl.getResource("job.jar/lib/lib2.jar"));

      // Check that the symlink for the renaming was created in the cwd;
      File symlinkFile = new File("distributed.first.symlink");
      Assert.assertTrue(symlinkFile.exists());
      Assert.assertEquals(1, symlinkFile.length());
      
      // Check that the symlink for the Job Jar was created in the cwd and
      // points to the extracted directory
      File jobJarDir = new File("job.jar");
      if (Shell.WINDOWS) {
        Assert.assertTrue(isWindowsSymlinkedDirectory(jobJarDir));
      } else {
        Assert.assertTrue(FileUtils.isSymlink(jobJarDir));
        Assert.assertTrue(jobJarDir.isDirectory());
      }
    }

    /**
     * Used on Windows to determine if the specified file is a symlink that
     * targets a directory.  On most platforms, these checks can be done using
     * commons-io.  On Windows, the commons-io implementation is unreliable and
     * always returns false.  Instead, this method checks the output of the dir
     * command.  After migrating to Java 7, this method can be removed in favor
     * of the new method java.nio.file.Files.isSymbolicLink, which is expected to
     * work cross-platform.
     * 
     * @param file File to check
     * @return boolean true if the file is a symlink that targets a directory
     * @throws IOException thrown for any I/O error
     */
    private static boolean isWindowsSymlinkedDirectory(File file)
        throws IOException {
      String dirOut = Shell.execCommand("cmd", "/c", "dir",
        file.getAbsoluteFile().getParent());
      StringReader sr = new StringReader(dirOut);
      BufferedReader br = new BufferedReader(sr);
      try {
        String line = br.readLine();
        while (line != null) {
          line = br.readLine();
          if (line.contains(file.getName()) && line.contains("<SYMLINKD>")) {
            return true;
          }
        }
        return false;
      } finally {
        IOUtils.closeStream(br);
        IOUtils.closeStream(sr);
      }
    }

    /**
     * Returns a mapping of the final component of each path to the corresponding
     * Path instance.  This assumes that every given Path has a unique string in
     * the final path component, which is true for these tests.
     * 
     * @param paths Path[] to map
     * @return Map<String, Path> mapping the final component of each path to the
     *   corresponding Path instance
     */
    private static Map<String, Path> pathsToMap(Path[] paths) {
      Map<String, Path> map = new HashMap<String, Path>();
      for (Path path: paths) {
        map.put(path.getName(), path);
      }
      return map;
    }
  }

  private void testDistributedCache(String jobJarPath, boolean withWildcard)
      throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
           + " not found. Not running test.");
      return;
    }

    // Create a temporary file of length 1.
    Path first = createTempFile("distributed.first", "x");
    // Create three jars with a single file inside them.
    Path second =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.second.jar"), 2);
    Path third =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.third.jar"), 3);
    Path fourth =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.fourth.jar"), 4);

    Job job = Job.getInstance(mrCluster.getConfig());

    // Set the job jar to a new "dummy" jar so we can check that its extracted 
    // properly
    job.setJar(jobJarPath);

    if (withWildcard) {
      // If testing with wildcards, upload the DistributedCacheChecker into HDFS
      // and add the directory as a wildcard.
      Path libs = new Path("testLibs");
      Path wildcard = remoteFs.makeQualified(new Path(libs, "*"));

      remoteFs.mkdirs(libs);
      remoteFs.copyFromLocalFile(third, libs);
      job.addCacheFile(wildcard.toUri());
    } else {
      // Otherwise add the DistributedCacheChecker directly to the classpath.
      // Because the job jar is a "dummy" jar, we need to include the jar with
      // DistributedCacheChecker or it won't be able to find it
      Path distributedCacheCheckerJar = new Path(
              JarFinder.getJar(DistributedCacheChecker.class));
      job.addFileToClassPath(localFs.makeQualified(distributedCacheCheckerJar));
    }
    
    job.setMapperClass(DistributedCacheChecker.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    FileInputFormat.setInputPaths(job, first);
    // Creates the Job Configuration
    job.addCacheFile(
        new URI(first.toUri().toString() + "#distributed.first.symlink"));
    job.addFileToClassPath(second);
    // The AppMaster jar itself
    job.addFileToClassPath(
            APP_JAR.makeQualified(localFs.getUri(), APP_JAR.getParent())); 
    job.addArchiveToClassPath(third);
    job.addCacheArchive(fourth.toUri());
    job.setMaxMapAttempts(1); // speed up failures

    job.submit();
    String trackingUrl = job.getTrackingURL();
    String jobId = job.getJobID().toString();
    Assert.assertTrue(job.waitForCompletion(false));
    Assert.assertTrue("Tracking URL was " + trackingUrl +
                      " but didn't Match Job ID " + jobId ,
          trackingUrl.endsWith(jobId.substring(jobId.lastIndexOf("_")) + "/"));
  }
  
  private void testDistributedCache(boolean withWildcard) throws Exception {
    // Test with a local (file:///) Job Jar
    Path localJobJarPath = makeJobJarWithLib(TEST_ROOT_DIR.toUri().toString());
    testDistributedCache(localJobJarPath.toUri().toString(), withWildcard);
    
    // Test with a remote (hdfs://) Job Jar
    Path remoteJobJarPath = new Path(remoteFs.getUri().toString() + "/",
            localJobJarPath.getName());
    remoteFs.moveFromLocalFile(localJobJarPath, remoteJobJarPath);
    File localJobJarFile = new File(localJobJarPath.toUri().toString());
    if (localJobJarFile.exists()) {     // just to make sure
        localJobJarFile.delete();
    }
    testDistributedCache(remoteJobJarPath.toUri().toString(), withWildcard);
  }

  @Test (timeout = 300000)
  public void testDistributedCache() throws Exception {
    testDistributedCache(false);
  }

  @Test (timeout = 300000)
  public void testDistributedCacheWithWildcards() throws Exception {
    testDistributedCache(true);
  }

  @Test(timeout = 120000)
  public void testThreadDumpOnTaskTimeout() throws IOException,
      InterruptedException, ClassNotFoundException {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    final SleepJob sleepJob = new SleepJob();
    final JobConf sleepConf = new JobConf(mrCluster.getConfig());
    sleepConf.setLong(MRJobConfig.TASK_TIMEOUT, 3 * 1000L);
    sleepConf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    sleepJob.setConf(sleepConf);
    if (this instanceof TestUberAM) {
      sleepConf.setInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS,
          30 * 1000);
    }
    // sleep for 10 seconds to trigger a kill with thread dump
    final Job job = sleepJob.createJob(1, 0, 10 * 60 * 1000L, 1, 0L, 0);
    job.setJarByClass(SleepJob.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.waitForCompletion(true);
    final JobId jobId = TypeConverter.toYarn(job.getJobID());
    final ApplicationId appID = jobId.getAppId();
    int pollElapsed = 0;
    while (true) {
      Thread.sleep(1000);
      pollElapsed += 1000;
      if (TERMINAL_RM_APP_STATES.contains(mrCluster.getResourceManager()
          .getRMContext().getRMApps().get(appID).getState())) {
        break;
      }
      if (pollElapsed >= 60000) {
        LOG.warn("application did not reach terminal state within 60 seconds");
        break;
      }
    }

    // Job finished, verify logs
    //

    final String appIdStr = appID.toString();
    final String appIdSuffix = appIdStr.substring("application_".length(),
        appIdStr.length());
    final String containerGlob = "container_" + appIdSuffix + "_*_*";
    final String syslogGlob = appIdStr
        + Path.SEPARATOR + containerGlob
        + Path.SEPARATOR + TaskLog.LogName.SYSLOG;
    int numAppMasters = 0;
    int numMapTasks = 0;

    for (int i = 0; i < NUM_NODE_MGRS; i++) {
      final Configuration nmConf = mrCluster.getNodeManager(i).getConfig();
      for (String logDir :
               nmConf.getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)) {
        final Path absSyslogGlob =
            new Path(logDir + Path.SEPARATOR + syslogGlob);
        LOG.info("Checking for glob: " + absSyslogGlob);
        for (FileStatus syslog : localFs.globStatus(absSyslogGlob)) {
          boolean foundAppMaster = false;
          boolean foundThreadDump = false;

          // Determine the container type
          final BufferedReader syslogReader = new BufferedReader(
              new InputStreamReader(localFs.open(syslog.getPath())));
          try {
            for (String line; (line = syslogReader.readLine()) != null; ) {
              if (line.contains(MRAppMaster.class.getName())) {
                foundAppMaster = true;
                break;
              }
            }
          } finally {
            syslogReader.close();
          }

          // Check for thread dump in stdout
          final Path stdoutPath = new Path(syslog.getPath().getParent(),
              TaskLog.LogName.STDOUT.toString());
          final BufferedReader stdoutReader = new BufferedReader(
              new InputStreamReader(localFs.open(stdoutPath)));
          try {
            for (String line; (line = stdoutReader.readLine()) != null; ) {
              if (line.contains("Full thread dump")) {
                foundThreadDump = true;
                break;
              }
            }
          } finally {
            stdoutReader.close();
          }

          if (foundAppMaster) {
            numAppMasters++;
            if (this instanceof TestUberAM) {
              Assert.assertTrue("No thread dump", foundThreadDump);
            } else {
              Assert.assertFalse("Unexpected thread dump", foundThreadDump);
            }
          } else {
            numMapTasks++;
            Assert.assertTrue("No thread dump", foundThreadDump);
          }
        }
      }
    }

    // Make sure we checked non-empty set
    //
    Assert.assertEquals("No AppMaster log found!", 1, numAppMasters);
    if (sleepConf.getBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false)) {
      Assert.assertSame("MapTask log with uber found!", 0, numMapTasks);
    } else {
      Assert.assertSame("No MapTask log found!", 1, numMapTasks);
    }
  }

  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(TEST_ROOT_DIR, filename);
    FSDataOutputStream os = localFs.create(path);
    os.writeBytes(contents);
    os.close();
    localFs.setPermission(path, new FsPermission("700"));
    return path;
  }

  private Path makeJar(Path p, int index) throws FileNotFoundException,
      IOException {
    FileOutputStream fos =
        new FileOutputStream(new File(p.toUri().getPath()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
    jos.putNextEntry(ze);
    jos.write(("inside the jar!" + index).getBytes());
    jos.closeEntry();
    jos.close();
    localFs.setPermission(p, new FsPermission("700"));
    return p;
  }
  
  private Path makeJobJarWithLib(String testDir) throws FileNotFoundException, 
      IOException{
    Path jobJarPath = new Path(testDir, "thejob.jar");
    FileOutputStream fos =
        new FileOutputStream(new File(jobJarPath.toUri().getPath()));
    JarOutputStream jos = new JarOutputStream(fos);
    // Have to put in real jar files or it will complain
    createAndAddJarToJar(jos, new File(
            new Path(testDir, "lib1.jar").toUri().getPath()));
    createAndAddJarToJar(jos, new File(
            new Path(testDir, "lib2.jar").toUri().getPath()));
    jos.close();
    localFs.setPermission(jobJarPath, new FsPermission("700"));
    return jobJarPath;
  }
  
  private void createAndAddJarToJar(JarOutputStream jos, File jarFile) 
          throws FileNotFoundException, IOException {
    FileOutputStream fos2 = new FileOutputStream(jarFile);
    JarOutputStream jos2 = new JarOutputStream(fos2);
    // Have to have at least one entry or it will complain
    ZipEntry ze = new ZipEntry("lib1.inside");
    jos2.putNextEntry(ze);
    jos2.closeEntry();
    jos2.close();
    ze = new ZipEntry("lib/" + jarFile.getName());
    jos.putNextEntry(ze);
    FileInputStream in = new FileInputStream(jarFile);
    byte buf[] = new byte[1024];
    int numRead;
    do {
       numRead = in.read(buf);
       if (numRead >= 0) {
           jos.write(buf, 0, numRead);
       }
    } while (numRead != -1);
    in.close();
    jos.closeEntry();
    jarFile.delete();
  }

  public static class ConfVerificationMapper extends SleepMapper {
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      final Configuration conf = context.getConfiguration();
      // check if the job classloader is enabled and verify the TCCL
      if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (!(tccl instanceof ApplicationClassLoader)) {
          throw new IOException("TCCL expected: " +
              ApplicationClassLoader.class.getName() + ", actual: " +
              tccl.getClass().getName());
        }
      }
      final String ioSortMb = conf.get(MRJobConfig.IO_SORT_MB);
      if (!TEST_IO_SORT_MB.equals(ioSortMb)) {
        throw new IOException("io.sort.mb expected: " + TEST_IO_SORT_MB
            + ", actual: "  + ioSortMb);
      }
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
      super.map(key, value, context);
      for (int i = 0; i < 100; i++) {
        context.getCounter("testCounterGroup-" + i,
            "testCounter").increment(1);
      }
    }
  }
}
