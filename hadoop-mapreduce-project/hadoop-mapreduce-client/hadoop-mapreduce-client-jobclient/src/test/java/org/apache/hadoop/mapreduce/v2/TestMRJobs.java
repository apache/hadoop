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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
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
import org.apache.hadoop.SleepJob;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRJobs {

  private static final Log LOG = LogFactory.getLog(TestMRJobs.class);

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
    try {
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
  }

  private static Path TEST_ROOT_DIR = new Path("target",
      TestMRJobs.class.getName() + "-tmpDir").makeQualified(localFs);
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
  private static final String OUTPUT_ROOT_DIR = "/tmp/" +
    TestMRJobs.class.getSimpleName();

  @BeforeClass
  public static void setup() throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestMRJobs.class.getName(), 3);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
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

  @Test (timeout = 30000)
  public void testSleepJob() throws IOException, InterruptedException,
      ClassNotFoundException { 
    LOG.info("\n\n\nStarting testSleepJob().");

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

    int numReduces = sleepConf.getInt("TestMRJobs.testSleepJob.reduces", 2); // or sleepConf.getConfig().getInt(MRJobConfig.NUM_REDUCES, 2);
   
    // job with 3 maps (10s) and numReduces reduces (5s), 1 "record" each:
    Job job = sleepJob.createJob(3, numReduces, 10000, 1, 5000, 1);

    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setJarByClass(SleepJob.class);
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

  protected void verifySleepJobCounters(Job job) throws InterruptedException,
      IOException {
    Counters counters = job.getCounters();
    Assert.assertEquals(3, counters.findCounter(JobCounter.OTHER_LOCAL_MAPS)
        .getValue());
    Assert.assertEquals(3, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
        .getValue());
    Assert.assertEquals(2,
        counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES).getValue());
    Assert
        .assertTrue(counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS) != null
            && counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue() != 0);
    Assert
        .assertTrue(counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS) != null
            && counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue() != 0);
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

  @Test (timeout = 30000)
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
    Assert
        .assertTrue(counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS) != null
            && counters.findCounter(JobCounter.SLOTS_MILLIS_MAPS).getValue() != 0);
  }

  @Test (timeout = 30000)
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

    Job job = new Job(myConf);

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

  //@Test (timeout = 30000)
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

  public static class DistributedCacheChecker extends
      Mapper<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path[] files = context.getLocalCacheFiles();
      Path[] archives = context.getLocalCacheArchives();

      // Check that 4 (2 + appjar + DistrubutedCacheChecker jar) files 
      // and 2 archives are present
      Assert.assertEquals(4, files.length);
      Assert.assertEquals(2, archives.length);

      // Check lengths of the files
      Map<String, Path> filesMap = pathsToMap(files);
      Assert.assertTrue(filesMap.containsKey("distributed.first.symlink"));
      Assert.assertEquals(1, localFs.getFileStatus(
        filesMap.get("distributed.first.symlink")).getLen());
      Assert.assertTrue(filesMap.containsKey("distributed.second.jar"));
      Assert.assertTrue(localFs.getFileStatus(
        filesMap.get("distributed.second.jar")).getLen() > 1);

      // Check extraction of the archive
      Map<String, Path> archivesMap = pathsToMap(archives);
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
      Assert.assertTrue(FileUtils.isSymlink(jobJarDir));
      Assert.assertTrue(jobJarDir.isDirectory());
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

  public void _testDistributedCache(String jobJarPath) throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
           + " not found. Not running test.");
      return;
    }

    // Create a temporary file of length 1.
    Path first = createTempFile("distributed.first", "x");
    // Create two jars with a single file inside them.
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
    // Because the job jar is a "dummy" jar, we need to include the jar with
    // DistributedCacheChecker or it won't be able to find it
    Path distributedCacheCheckerJar = new Path(
            JarFinder.getJar(DistributedCacheChecker.class));
    job.addFileToClassPath(distributedCacheCheckerJar.makeQualified(
            localFs.getUri(), distributedCacheCheckerJar.getParent()));
    
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
  
  @Test (timeout = 30000)
  public void testDistributedCache() throws Exception {
    // Test with a local (file:///) Job Jar
    Path localJobJarPath = makeJobJarWithLib(TEST_ROOT_DIR.toUri().toString());
    _testDistributedCache(localJobJarPath.toUri().toString());
    
    // Test with a remote (hdfs://) Job Jar
    Path remoteJobJarPath = new Path(remoteFs.getUri().toString() + "/",
            localJobJarPath.getName());
    remoteFs.moveFromLocalFile(localJobJarPath, remoteJobJarPath);
    File localJobJarFile = new File(localJobJarPath.toUri().toString());
    if (localJobJarFile.exists()) {     // just to make sure
        localJobJarFile.delete();
    }
    _testDistributedCache(remoteJobJarPath.toUri().toString());
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
}
