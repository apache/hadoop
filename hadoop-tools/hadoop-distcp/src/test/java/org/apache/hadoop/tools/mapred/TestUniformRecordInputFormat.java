/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpContext;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.StubContext;
import org.apache.hadoop.tools.util.DistCpUtils;

import static org.assertj.core.api.Assertions.assertThat;


public class TestUniformRecordInputFormat extends AbstractHadoopTestBase {
  private static MiniDFSCluster cluster;
  private static final int N_FILES = 20;
  private static final int SIZEOF_EACH_FILE = 1024;
  private static final Random random = new Random();
  private static int totalFileSize = 0;

  private static final Credentials CREDENTIALS = new Credentials();


  @BeforeClass
  public static void setupClass() throws Exception {
    cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1)
        .format(true).build();
    totalFileSize = 0;

    for (int i = 0; i < N_FILES; ++i)
      totalFileSize += createFile("/tmp/source/" + String.valueOf(i), SIZEOF_EACH_FILE);
  }

  private static DistCpOptions getOptions(int nMaps) throws Exception {
    Path sourcePath = new Path(cluster.getFileSystem().getUri().toString()
        + "/tmp/source");
    Path targetPath = new Path(cluster.getFileSystem().getUri().toString()
        + "/tmp/target");

    List<Path> sourceList = new ArrayList<Path>();
    sourceList.add(sourcePath);
    return new DistCpOptions.Builder(sourceList, targetPath)
        .maxMaps(nMaps)
        .build();
  }

  private static int createFile(String path, int fileSize) throws Exception {
    FileSystem fileSystem = null;
    DataOutputStream outputStream = null;
    try {
      fileSystem = cluster.getFileSystem();
      outputStream = fileSystem.create(new Path(path), true, 0);
      int size = (int) Math.ceil(fileSize + (1 - random.nextFloat()) * fileSize);
      outputStream.write(new byte[size]);
      return size;
    } finally {
      IOUtils.cleanupWithLogger(null, outputStream);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public void testGetSplits(int nMaps) throws Exception {
    DistCpContext context = new DistCpContext(getOptions(nMaps));
    Configuration configuration = new Configuration();
    configuration.set(MRJobConfig.NUM_MAPS, String.valueOf(context.getMaxMaps()));
    Path listFile = new Path(cluster.getFileSystem().getUri().toString()
        + "/tmp/testGetSplits_2/fileList.seq");
    CopyListing.getCopyListing(configuration, CREDENTIALS, context)
        .buildListing(listFile, context);

    JobContext jobContext = new JobContextImpl(configuration, new JobID());
    UniformRecordInputFormat uniformRecordInputFormat = new UniformRecordInputFormat();
    List<InputSplit> splits
        = uniformRecordInputFormat.getSplits(jobContext);

    long totalRecords = DistCpUtils.getLong(configuration, DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
    long recordPerMap = totalRecords / nMaps;


    checkSplits(listFile, splits);

    int doubleCheckedTotalSize = 0;
    for (int i = 0; i < splits.size(); ++i) {
      InputSplit split = splits.get(i);
      int currentSplitSize = 0;
      RecordReader<Text, CopyListingFileStatus> recordReader =
          uniformRecordInputFormat.createRecordReader(split, null);
      StubContext stubContext = new StubContext(jobContext.getConfiguration(),
          recordReader, 0);
      final TaskAttemptContext taskAttemptContext
          = stubContext.getContext();
      recordReader.initialize(split, taskAttemptContext);
      long recordCnt = 0;
      while (recordReader.nextKeyValue()) {
        recordCnt++;
        Path sourcePath = recordReader.getCurrentValue().getPath();
        FileSystem fs = sourcePath.getFileSystem(configuration);
        FileStatus fileStatus[] = fs.listStatus(sourcePath);
        // If the fileStatus is a file which fileStatus[] must be 1,
        // then add the size of file, otherwise it is a directory skip.
        // For all files under the directory will be in the sequence file too.
        if (fileStatus.length > 1) {
          continue;
        }
        currentSplitSize += fileStatus[0].getLen();
      }

      assertThat(recordCnt)
          .describedAs("record count")
          .isGreaterThanOrEqualTo(recordPerMap)
          .isLessThanOrEqualTo(recordPerMap + 1);
      doubleCheckedTotalSize += currentSplitSize;
    }

    assertThat(totalFileSize).isEqualTo(doubleCheckedTotalSize);
  }

  private void checkSplits(Path listFile, List<InputSplit> splits) throws IOException {
    long lastEnd = 0;

    //Verify if each split's start is matching with the previous end and
    //we are not missing anything
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      long start = fileSplit.getStart();
      assertThat(lastEnd)
          .isEqualTo(start)
          .withFailMessage("The end of last file is not equals to the begin of current file.");
      lastEnd = start + fileSplit.getLength();
    }

    //Verify there is nothing more to read from the input file
    try (SequenceFile.Reader reader
             = new SequenceFile.Reader(cluster.getFileSystem().getConf(),
        SequenceFile.Reader.file(listFile))) {
      reader.seek(lastEnd);
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();
      assertThat(reader.next(srcRelPath, srcFileStatus))
          .isFalse()
          .withFailMessage("The reader is expected to reach end of file, but it doesn't.");
    }
  }

  @Test
  public void testGetSplits() throws Exception {
    for (int i = 1; i < N_FILES; ++i)
      testGetSplits(i);
  }
}
