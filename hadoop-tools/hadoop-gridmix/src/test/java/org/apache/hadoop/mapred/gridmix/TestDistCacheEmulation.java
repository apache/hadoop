/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validate emulation of distributed cache load in gridmix simulated jobs.
 * 
 */
public class TestDistCacheEmulation {

  private DistributedCacheEmulator dce = null;

  @BeforeClass
  public static void init() throws IOException {
    GridmixTestUtils.initCluster(TestDistCacheEmulation.class);
  }

  @AfterClass
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }

  

  /**
   * Configures 5 HDFS-based dist cache files and 1 local-FS-based dist cache
   * file in the given Configuration object <code>conf</code>.
   * 
   * @param conf
   *          configuration where dist cache config properties are to be set
   * @return array of sorted HDFS-based distributed cache file sizes
   * @throws IOException
   */
  private long[] configureDummyDistCacheFiles(Configuration conf)
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set("user.name", user);
    
    // Set some dummy dist cache files in gridmix configuration so that they go
    // into the configuration of JobStory objects.
    String[] distCacheFiles = { "hdfs:///tmp/file1.txt",
        "/tmp/" + user + "/.staging/job_1/file2.txt",
        "hdfs:///user/user1/file3.txt", "/home/user2/file4.txt",
        "subdir1/file5.txt", "subdir2/file6.gz" };

    String[] fileSizes = { "400", "2500", "700", "1200", "1500", "500" };

    String[] visibilities = { "true", "false", "false", "true", "true", "false" };
    String[] timeStamps = { "1234", "2345", "34567", "5434", "125", "134" };

    // DistributedCache.setCacheFiles(fileCaches, conf);
    conf.setStrings(MRJobConfig.CACHE_FILES, distCacheFiles);
    conf.setStrings(MRJobConfig.CACHE_FILES_SIZES, fileSizes);
    conf.setStrings(JobContext.CACHE_FILE_VISIBILITIES, visibilities);
    conf.setStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS, timeStamps);

    // local FS based dist cache file whose path contains <user>/.staging is
    // not created on HDFS. So file size 2500 is not added to sortedFileSizes.
    long[] sortedFileSizes = new long[] { 1500, 1200, 700, 500, 400 };
    return sortedFileSizes;
  }

  /**
   * Runs setupGenerateDistCacheData() on a new DistrbutedCacheEmulator and and
   * returns the jobConf. Fills the array <code>sortedFileSizes</code> that can
   * be used for validation. Validation of exit code from
   * setupGenerateDistCacheData() is done.
   * 
   * @param generate
   *          true if -generate option is specified
   * @param sortedFileSizes
   *          sorted HDFS-based distributed cache file sizes
   * @throws IOException
   * @throws InterruptedException
   */
  private Configuration runSetupGenerateDistCacheData(boolean generate,
      long[] sortedFileSizes, int expectedExitCode) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    long[] fileSizes = configureDummyDistCacheFiles(conf);
    System.arraycopy(fileSizes, 0, sortedFileSizes, 0, fileSizes.length);

    // Job stories of all 3 jobs will have same dist cache files in their
    // configurations
    final int numJobs = 3;
    DebugJobProducer jobProducer = new DebugJobProducer(numJobs, conf);

    Configuration jobConf = GridmixTestUtils.mrvl.getConfig();
    Path ioPath = new Path("testSetupGenerateDistCacheData")
        .makeQualified(GridmixTestUtils.dfs.getUri(),GridmixTestUtils.dfs.getWorkingDirectory());
    
    FileSystem fs = FileSystem.get(jobConf);
    if (fs.exists(ioPath)) {
      fs.delete(ioPath, true);
    }
    FileSystem.mkdirs(fs, ioPath, new FsPermission((short) 777));

    dce = createDistributedCacheEmulator(jobConf, ioPath, generate);
    int exitCode = dce.setupGenerateDistCacheData(jobProducer);
    assertEquals("setupGenerateDistCacheData failed.", expectedExitCode,
        exitCode);

    // reset back
    resetDistCacheConfigProperties(jobConf);
    return jobConf;
  }

  /**
   * Reset the config properties related to Distributed Cache in the given job
   * configuration <code>jobConf</code>.
   * 
   * @param jobConf
   *          job configuration
   */
  private void resetDistCacheConfigProperties(Configuration jobConf) {
    // reset current/latest property names
    jobConf.setStrings(MRJobConfig.CACHE_FILES, "");
    jobConf.setStrings(MRJobConfig.CACHE_FILES_SIZES, "");
    jobConf.setStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS, "");
    jobConf.setStrings(JobContext.CACHE_FILE_VISIBILITIES, "");
    // reset old property names
    jobConf.setStrings("mapred.cache.files", "");
    jobConf.setStrings("mapred.cache.files.filesizes", "");
    jobConf.setStrings("mapred.cache.files.visibilities", "");
    jobConf.setStrings("mapred.cache.files.timestamps", "");
  }

  /**
   * Validate GenerateDistCacheData job if it creates dist cache files properly.
   * 
   * @throws Exception
   */
  @Test (timeout=10000)
  public void testGenerateDistCacheData() throws Exception {
    long[] sortedFileSizes = new long[5];
    Configuration jobConf = runSetupGenerateDistCacheData(true, sortedFileSizes,0);
    GridmixJob gridmixJob = new GenerateDistCacheData(jobConf);
    Job job = gridmixJob.call();
    assertEquals("Number of reduce tasks in GenerateDistCacheData is not 0.",
        0, job.getNumReduceTasks());
  }

  /**
   * Validate setupGenerateDistCacheData by validating <li>permissions of the
   * distributed cache directories and <li>content of the generated sequence
   * file. This includes validation of dist cache file paths and their file
   * sizes.
   */
  private void validateSetupGenDC(Configuration jobConf, long[] sortedFileSizes)
      throws IOException, InterruptedException {
    // build things needed for validation
    long sumOfFileSizes = 0;
    for (int i = 0; i < sortedFileSizes.length; i++) {
      sumOfFileSizes += sortedFileSizes[i];
    }

    FileSystem fs = FileSystem.get(jobConf);
    assertEquals("Number of distributed cache files to be generated is wrong.",
        sortedFileSizes.length,
        jobConf.getInt(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_COUNT, -1));
    assertEquals("Total size of dist cache files to be generated is wrong.",
        sumOfFileSizes,
        jobConf.getLong(GenerateDistCacheData.GRIDMIX_DISTCACHE_BYTE_COUNT, -1));
    Path filesListFile = new Path(
        jobConf.get(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_LIST));
    FileStatus stat = fs.getFileStatus(filesListFile);
    assertEquals("Wrong permissions of dist Cache files list file "
        + filesListFile, new FsPermission((short) 0644), stat.getPermission());

    InputSplit split = new FileSplit(filesListFile, 0, stat.getLen(),
        (String[]) null);
    TaskAttemptContext taskContext = MapReduceTestUtil
        .createDummyMapTaskAttemptContext(jobConf);
    RecordReader<LongWritable, BytesWritable> reader = new GenerateDistCacheData.GenDCDataFormat()
        .createRecordReader(split, taskContext);
    MapContext<LongWritable, BytesWritable, NullWritable, BytesWritable> mapContext = new MapContextImpl<LongWritable, BytesWritable, NullWritable, BytesWritable>(
        jobConf, taskContext.getTaskAttemptID(), reader, null, null,
        MapReduceTestUtil.createDummyReporter(), split);
    reader.initialize(split, mapContext);

    // start validating setupGenerateDistCacheData
    doValidateSetupGenDC(reader, fs, sortedFileSizes);
  }

  /**
   * Validate setupGenerateDistCacheData by validating <li>permissions of the
   * distributed cache directory and <li>content of the generated sequence file.
   * This includes validation of dist cache file paths and their file sizes.
   */
  private void doValidateSetupGenDC(
      RecordReader<LongWritable, BytesWritable> reader, FileSystem fs,
      long[] sortedFileSizes) throws IOException, InterruptedException {

    // Validate permissions of dist cache directory
    Path distCacheDir = dce.getDistributedCacheDir();
    assertEquals(
        "Wrong permissions for distributed cache dir " + distCacheDir,
        fs.getFileStatus(distCacheDir).getPermission().getOtherAction()
            .and(FsAction.EXECUTE), FsAction.EXECUTE);

    // Validate the content of the sequence file generated by
    // dce.setupGenerateDistCacheData().
    LongWritable key = new LongWritable();
    BytesWritable val = new BytesWritable();
    for (int i = 0; i < sortedFileSizes.length; i++) {
      assertTrue("Number of files written to the sequence file by "
          + "setupGenerateDistCacheData is less than the expected.",
          reader.nextKeyValue());
      key = reader.getCurrentKey();
      val = reader.getCurrentValue();
      long fileSize = key.get();
      String file = new String(val.getBytes(), 0, val.getLength());

      // Dist Cache files should be sorted based on file size.
      assertEquals("Dist cache file size is wrong.", sortedFileSizes[i],
          fileSize);

      // Validate dist cache file path.

      // parent dir of dist cache file
      Path parent = new Path(file).getParent().makeQualified(fs.getUri(),fs.getWorkingDirectory());
      // should exist in dist cache dir
      assertTrue("Public dist cache file path is wrong.",
          distCacheDir.equals(parent));
    }
  }

  /**
   * Test if DistributedCacheEmulator's setup of GenerateDistCacheData is
   * working as expected.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=5000)
  public void testSetupGenerateDistCacheData() throws IOException,
      InterruptedException {
    long[] sortedFileSizes = new long[5];
    Configuration jobConf = runSetupGenerateDistCacheData(true, sortedFileSizes,0);
    validateSetupGenDC(jobConf, sortedFileSizes);

    // Verify if correct exit code is seen when -generate option is missing and
    // distributed cache files are missing in the expected path.
    runSetupGenerateDistCacheData(false, sortedFileSizes,1);
  }

  /**
   * Create DistributedCacheEmulator object and do the initialization by calling
   * init() on it with dummy trace. Also configure the pseudo local FS.
   */
  private DistributedCacheEmulator createDistributedCacheEmulator(
      Configuration conf, Path ioPath, boolean generate) throws IOException {
    DistributedCacheEmulator dce = new DistributedCacheEmulator(conf, ioPath);
    JobCreator jobCreator = JobCreator.getPolicy(conf, JobCreator.LOADJOB);
    jobCreator.setDistCacheEmulator(dce);
    dce.init("dummytrace", jobCreator, generate);
    return dce;
  }

  /**
   * Test the configuration property for disabling/enabling emulation of
   * distributed cache load.
   */
  @Test (timeout=5000)
  public void testDistCacheEmulationConfigurability() throws IOException {
    // Configuration conf = new Configuration();
    Configuration jobConf = GridmixTestUtils.mrvl.getConfig();
    Path ioPath = new Path("testDistCacheEmulationConfigurability")
        .makeQualified(GridmixTestUtils.dfs.getUri(),GridmixTestUtils.dfs.getWorkingDirectory());
    FileSystem fs = FileSystem.get(jobConf);
    FileSystem.mkdirs(fs, ioPath, new FsPermission((short) 0777));

    // default config
    dce = createDistributedCacheEmulator(jobConf, ioPath, false);
    assertTrue("Default configuration of "
        + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
        + " is wrong.", dce.shouldEmulateDistCacheLoad());

    // config property set to false
    jobConf.setBoolean(
        DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE, false);
    dce = createDistributedCacheEmulator(jobConf, ioPath, false);
    assertFalse("Disabling of emulation of distributed cache load by setting "
        + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
        + " to false is not working.", dce.shouldEmulateDistCacheLoad());
  }
/** 
 * test method configureDistCacheFiles
 * 
 */
  @Test (timeout=50000)
  public void testDistCacheEmulator() throws Exception {

    Configuration conf = new Configuration();
    configureDummyDistCacheFiles(conf);
    File ws = new File("target" + File.separator + this.getClass().getName());
    Path ioPath = new Path(ws.getAbsolutePath());

    DistributedCacheEmulator dce = new DistributedCacheEmulator(conf, ioPath);
    JobConf jobConf = new JobConf(conf);
    jobConf.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    File fin=new File("src"+File.separator+"test"+File.separator+"resources"+File.separator+"data"+File.separator+"wordcount.json");
    dce.init(fin.getAbsolutePath(), JobCreator.LOADJOB, true);
    dce.configureDistCacheFiles(conf, jobConf);
    
    String[] caches=conf.getStrings(MRJobConfig.CACHE_FILES);
    String[] tmpfiles=conf.getStrings("tmpfiles");
    // this method should fill caches AND tmpfiles  from MRJobConfig.CACHE_FILES property 
    assertEquals(6, ((caches==null?0:caches.length)+(tmpfiles==null?0:tmpfiles.length)));
  }
}
