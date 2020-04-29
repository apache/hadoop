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

package org.apache.hadoop.mapreduce.split;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests that maxBlockLocations default value is sufficient for RS-10-4.
 */
public class TestJobSplitWriterWithEC {
  // This will ensure 14 block locations
  private ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies
      .getByID(SystemErasureCodingPolicies.RS_10_4_POLICY_ID);
  private static final int BLOCKSIZE = 1024 * 1024 * 10;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private Path submitDir;
  private Path testFile;

  @Before
  public void setup() throws Exception {
    Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    String namenodeDir = new File(MiniDFSCluster.getBaseDirectory(), "name").
        getAbsolutePath();
    hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
    hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
    hdfsConf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(hdfsConf).numDataNodes(15).build();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.setErasureCodingPolicy(new Path("/"), ecPolicy.getName());
    cluster.waitActive();

    conf = new Configuration();
    submitDir = new Path("/");
    testFile = new Path("/testfile");
    DFSTestUtil.writeFile(fs, testFile,
        StripedFileTestUtil.generateBytes(BLOCKSIZE));
    conf.set(FileInputFormat.INPUT_DIR,
        fs.getUri().toString() + testFile.toString());
  }

  @After
  public void after() {
    cluster.close();
  }

  @Test
  public void testMaxBlockLocationsNewSplitsWithErasureCoding()
      throws Exception {
    Job job = Job.getInstance(conf);
    final FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    final List<InputSplit> splits = fileInputFormat.getSplits(job);

    JobSplitWriter.createSplitFiles(submitDir, conf, fs, splits);

    validateSplitMetaInfo();
  }

  @Test
  public void testMaxBlockLocationsOldSplitsWithErasureCoding()
      throws Exception {
    JobConf jobConf = new JobConf(conf);
    org.apache.hadoop.mapred.TextInputFormat fileInputFormat
        = new org.apache.hadoop.mapred.TextInputFormat();
    fileInputFormat.configure(jobConf);
    final org.apache.hadoop.mapred.InputSplit[] splits =
        fileInputFormat.getSplits(jobConf, 1);

    JobSplitWriter.createSplitFiles(submitDir, conf, fs, splits);

    validateSplitMetaInfo();
  }

  private void validateSplitMetaInfo() throws IOException {
    JobSplit.TaskSplitMetaInfo[] splitInfo =
        SplitMetaInfoReader.readSplitMetaInfo(new JobID(), fs, conf,
            submitDir);

    assertEquals("Number of splits", 1, splitInfo.length);
    assertEquals("Number of block locations", 14,
        splitInfo[0].getLocations().length);
  }
}
