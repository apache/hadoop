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

package org.apache.hadoop.tools.mapred.lib;

import org.apache.hadoop.tools.DistCpConstants;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.StubContext;
import org.apache.hadoop.security.Credentials;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

public class TestDynamicInputFormat {
  private static final Log LOG = LogFactory.getLog(TestDynamicInputFormat.class);
  private static MiniDFSCluster cluster;
  private static final int N_FILES = 1000;
  private static final int NUM_SPLITS = 7;

  private static final Credentials CREDENTIALS = new Credentials();

  private static List<String> expectedFilePaths = new ArrayList<String>(N_FILES);

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConfigurationForCluster())
                  .numDataNodes(1).format(true).build();

    for (int i=0; i<N_FILES; ++i)
      createFile("/tmp/source/" + String.valueOf(i));
    FileSystem fileSystem = cluster.getFileSystem();
    expectedFilePaths.add(fileSystem.listStatus(
        new Path("/tmp/source/0"))[0].getPath().getParent().toString());
  }

  private static Configuration getConfigurationForCluster() {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data",
                       "target/tmp/build/TEST_DYNAMIC_INPUT_FORMAT/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private static DistCpOptions getOptions() throws Exception {
    Path sourcePath = new Path(cluster.getFileSystem().getUri().toString()
            + "/tmp/source");
    Path targetPath = new Path(cluster.getFileSystem().getUri().toString()
            + "/tmp/target");

    List<Path> sourceList = new ArrayList<Path>();
    sourceList.add(sourcePath);
    DistCpOptions options = new DistCpOptions(sourceList, targetPath);
    options.setMaxMaps(NUM_SPLITS);
    return options;
  }

  private static void createFile(String path) throws Exception {
    FileSystem fileSystem = null;
    DataOutputStream outputStream = null;
    try {
      fileSystem = cluster.getFileSystem();
      outputStream = fileSystem.create(new Path(path), true, 0);
      expectedFilePaths.add(fileSystem.listStatus(
                                    new Path(path))[0].getPath().toString());
    }
    finally {
      IOUtils.cleanup(null, fileSystem, outputStream);
    }
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testGetSplits() throws Exception {
    DistCpOptions options = getOptions();
    Configuration configuration = new Configuration();
    configuration.set("mapred.map.tasks",
                      String.valueOf(options.getMaxMaps()));
    CopyListing.getCopyListing(configuration, CREDENTIALS, options).buildListing(
            new Path(cluster.getFileSystem().getUri().toString()
                    +"/tmp/testDynInputFormat/fileList.seq"), options);

    JobContext jobContext = new JobContextImpl(configuration, new JobID());
    DynamicInputFormat<Text, CopyListingFileStatus> inputFormat =
        new DynamicInputFormat<Text, CopyListingFileStatus>();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);

    int nFiles = 0;
    int taskId = 0;

    for (InputSplit split : splits) {
      RecordReader<Text, CopyListingFileStatus> recordReader =
           inputFormat.createRecordReader(split, null);
      StubContext stubContext = new StubContext(jobContext.getConfiguration(),
                                                recordReader, taskId);
      final TaskAttemptContext taskAttemptContext
         = stubContext.getContext();
      
      recordReader.initialize(splits.get(0), taskAttemptContext);
      float previousProgressValue = 0f;
      while (recordReader.nextKeyValue()) {
        CopyListingFileStatus fileStatus = recordReader.getCurrentValue();
        String source = fileStatus.getPath().toString();
        System.out.println(source);
        Assert.assertTrue(expectedFilePaths.contains(source));
        final float progress = recordReader.getProgress();
        Assert.assertTrue(progress >= previousProgressValue);
        Assert.assertTrue(progress >= 0.0f);
        Assert.assertTrue(progress <= 1.0f);
        previousProgressValue = progress;
        ++nFiles;
      }
      Assert.assertTrue(recordReader.getProgress() == 1.0f);

      ++taskId;
    }

    Assert.assertEquals(expectedFilePaths.size(), nFiles);
  }

  @Test
  public void testGetSplitRatio() throws Exception {
    Assert.assertEquals(1, DynamicInputFormat.getSplitRatio(1, 1000000000));
    Assert.assertEquals(2, DynamicInputFormat.getSplitRatio(11000000, 10));
    Assert.assertEquals(4, DynamicInputFormat.getSplitRatio(30, 700));
    Assert.assertEquals(2, DynamicInputFormat.getSplitRatio(30, 200));

    // Tests with negative value configuration
    Configuration conf = new Configuration();
    conf.setInt(DistCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE, -1);
    conf.setInt(DistCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL, -1);
    conf.setInt(DistCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK, -1);
    conf.setInt(DistCpConstants.CONF_LABEL_SPLIT_RATIO, -1);
    Assert.assertEquals(1,
        DynamicInputFormat.getSplitRatio(1, 1000000000, conf));
    Assert.assertEquals(2,
        DynamicInputFormat.getSplitRatio(11000000, 10, conf));
    Assert.assertEquals(4, DynamicInputFormat.getSplitRatio(30, 700, conf));
    Assert.assertEquals(2, DynamicInputFormat.getSplitRatio(30, 200, conf));

    // Tests with valid configuration
    conf.setInt(DistCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE, 100);
    conf.setInt(DistCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL, 30);
    conf.setInt(DistCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK, 10);
    conf.setInt(DistCpConstants.CONF_LABEL_SPLIT_RATIO, 53);
    Assert.assertEquals(53, DynamicInputFormat.getSplitRatio(3, 200, conf));
  }
}
