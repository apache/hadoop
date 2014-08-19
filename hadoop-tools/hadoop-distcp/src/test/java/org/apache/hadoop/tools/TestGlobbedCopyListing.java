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

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.security.Credentials;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestGlobbedCopyListing {

  private static MiniDFSCluster cluster;

  private static final Credentials CREDENTIALS = new Credentials();

  public static Map<String, String> expectedValues = new HashMap<String, String>();

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MiniDFSCluster.Builder(new Configuration()).build();
    createSourceData();
  }

  private static void createSourceData() throws Exception {
    mkdirs("/tmp/source/1");
    mkdirs("/tmp/source/2");
    mkdirs("/tmp/source/2/3");
    mkdirs("/tmp/source/2/3/4");
    mkdirs("/tmp/source/5");
    touchFile("/tmp/source/5/6");
    mkdirs("/tmp/source/7");
    mkdirs("/tmp/source/7/8");
    touchFile("/tmp/source/7/8/9");
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = null;
    try {
      fileSystem = cluster.getFileSystem();
      fileSystem.mkdirs(new Path(path));
      recordInExpectedValues(path);
    }
    finally {
      IOUtils.cleanup(null, fileSystem);
    }
  }

  private static void touchFile(String path) throws Exception {
    FileSystem fileSystem = null;
    DataOutputStream outputStream = null;
    try {
      fileSystem = cluster.getFileSystem();
      outputStream = fileSystem.create(new Path(path), true, 0);
      recordInExpectedValues(path);
    }
    finally {
      IOUtils.cleanup(null, fileSystem, outputStream);
    }
  }

  private static void recordInExpectedValues(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    Path sourcePath = new Path(fileSystem.getUri().toString() + path);
    expectedValues.put(sourcePath.toString(), DistCpUtils.getRelativePath(
        new Path("/tmp/source"), sourcePath));
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testRun() throws Exception {
    final URI uri = cluster.getFileSystem().getUri();
    final String pathString = uri.toString();
    Path fileSystemPath = new Path(pathString);
    Path source = new Path(fileSystemPath.toString() + "/tmp/source");
    Path target = new Path(fileSystemPath.toString() + "/tmp/target");
    Path listingPath = new Path(fileSystemPath.toString() + "/tmp/META/fileList.seq");
    DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
    options.setTargetPathExists(false);
    new GlobbedCopyListing(new Configuration(), CREDENTIALS).buildListing(listingPath, options);

    verifyContents(listingPath);
  }

  private void verifyContents(Path listingPath) throws Exception {
    SequenceFile.Reader reader = new SequenceFile.Reader(cluster.getFileSystem(),
                                              listingPath, new Configuration());
    Text key   = new Text();
    CopyListingFileStatus value = new CopyListingFileStatus();
    Map<String, String> actualValues = new HashMap<String, String>();
    while (reader.next(key, value)) {
      if (value.isDirectory() && key.toString().equals("")) {
        // ignore root with empty relPath, which is an entry to be 
        // used for preserving root attributes etc.
        continue;
      }
      actualValues.put(value.getPath().toString(), key.toString());
    }

    Assert.assertEquals(expectedValues.size(), actualValues.size());
    for (Map.Entry<String, String> entry : actualValues.entrySet()) {
      Assert.assertEquals(entry.getValue(), expectedValues.get(entry.getKey()));
    }
  }
}
