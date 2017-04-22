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

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpContext;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.StubContext;
import org.apache.hadoop.security.Credentials;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class TestUniformSizeInputFormat {
  private static MiniDFSCluster cluster;
  private static final int N_FILES = 20;
  private static final int SIZEOF_EACH_FILE=1024;
  private static final Random random = new Random();
  private static int totalFileSize = 0;

  private static final Credentials CREDENTIALS = new Credentials();


  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1)
                                          .format(true).build();
    totalFileSize = 0;

    for (int i=0; i<N_FILES; ++i)
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
    }
    finally {
      IOUtils.cleanup(null, fileSystem, outputStream);
    }
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  public void testGetSplits(int nMaps) throws Exception {
    DistCpContext context = new DistCpContext(getOptions(nMaps));
    Configuration configuration = new Configuration();
    configuration.set("mapred.map.tasks",
                      String.valueOf(context.getMaxMaps()));
    Path listFile = new Path(cluster.getFileSystem().getUri().toString()
        + "/tmp/testGetSplits_1/fileList.seq");
    CopyListing.getCopyListing(configuration, CREDENTIALS, context)
        .buildListing(listFile, context);

    JobContext jobContext = new JobContextImpl(configuration, new JobID());
    UniformSizeInputFormat uniformSizeInputFormat = new UniformSizeInputFormat();
    List<InputSplit> splits
            = uniformSizeInputFormat.getSplits(jobContext);

    int sizePerMap = totalFileSize/nMaps;

    checkSplits(listFile, splits);

    int doubleCheckedTotalSize = 0;
    int previousSplitSize = -1;
    for (int i=0; i<splits.size(); ++i) {
      InputSplit split = splits.get(i);
      int currentSplitSize = 0;
      RecordReader<Text, CopyListingFileStatus> recordReader =
        uniformSizeInputFormat.createRecordReader(split, null);
      StubContext stubContext = new StubContext(jobContext.getConfiguration(),
                                                recordReader, 0);
      final TaskAttemptContext taskAttemptContext
         = stubContext.getContext();
      recordReader.initialize(split, taskAttemptContext);
      while (recordReader.nextKeyValue()) {
        Path sourcePath = recordReader.getCurrentValue().getPath();
        FileSystem fs = sourcePath.getFileSystem(configuration);
        FileStatus fileStatus [] = fs.listStatus(sourcePath);
        if (fileStatus.length > 1) {
          continue;
        }
        currentSplitSize += fileStatus[0].getLen();
      }
      Assert.assertTrue(
           previousSplitSize == -1
               || Math.abs(currentSplitSize - previousSplitSize) < 0.1*sizePerMap
               || i == splits.size()-1);

      doubleCheckedTotalSize += currentSplitSize;
    }

    Assert.assertEquals(totalFileSize, doubleCheckedTotalSize);
  }

  private void checkSplits(Path listFile, List<InputSplit> splits) throws IOException {
    long lastEnd = 0;

    //Verify if each split's start is matching with the previous end and
    //we are not missing anything
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      long start = fileSplit.getStart();
      Assert.assertEquals(lastEnd, start);
      lastEnd = start + fileSplit.getLength();
    }

    //Verify there is nothing more to read from the input file
    SequenceFile.Reader reader
            = new SequenceFile.Reader(cluster.getFileSystem().getConf(),
                    SequenceFile.Reader.file(listFile));

    try {
      reader.seek(lastEnd);
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();
      Assert.assertFalse(reader.next(srcRelPath, srcFileStatus));
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  @Test
  public void testGetSplits() throws Exception {
    testGetSplits(9);
    for (int i=1; i<N_FILES; ++i)
      testGetSplits(i);
  }
}
