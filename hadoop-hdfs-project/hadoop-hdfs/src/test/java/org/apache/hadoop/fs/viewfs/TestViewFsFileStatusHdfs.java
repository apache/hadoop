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
package org.apache.hadoop.fs.viewfs;


/**
 * The FileStatus is being serialized in MR as jobs are submitted.
 * Since viewfs has overlayed ViewFsFileStatus, we ran into
 * serialization problems. THis test is test the fix.
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestViewFsFileStatusHdfs {
  
  static final String testfilename = "/tmp/testFileStatusSerialziation";
  static final String someFile = "/hdfstmp/someFileForTestGetFileChecksum";

  private static final FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper();
  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem vfs;
  
  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    fHdfs = cluster.getFileSystem();
    defaultWorkingDirectory = fHdfs.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fHdfs.mkdirs(defaultWorkingDirectory);

    // Setup the ViewFS to be used for all tests.
    Configuration conf = ViewFileSystemTestSetup.createConfig();
    ConfigUtil.addLink(conf, "/vfstmp", new URI(fHdfs.getUri() + "/hdfstmp"));
    ConfigUtil.addLink(conf, "/tmp", new URI(fHdfs.getUri() + "/tmp"));
    vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
  }

  @Test
  public void testFileStatusSerialziation()
      throws IOException, URISyntaxException {
   long len = fileSystemTestHelper.createFile(fHdfs, testfilename);
    FileStatus stat = vfs.getFileStatus(new Path(testfilename));
    assertEquals(len, stat.getLen());
    // check serialization/deserialization
    DataOutputBuffer dob = new DataOutputBuffer();
    stat.write(dob);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    FileStatus deSer = new FileStatus();
    deSer.readFields(dib);
    assertEquals(len, deSer.getLen());
  }

  @Test
  public void testGetFileChecksum() throws IOException, URISyntaxException {
    // Create two different files in HDFS
    fileSystemTestHelper.createFile(fHdfs, someFile);
    fileSystemTestHelper.createFile(fHdfs, fileSystemTestHelper
      .getTestRootPath(fHdfs, someFile + "other"), 1, 512);
    // Get checksum through ViewFS
    FileChecksum viewFSCheckSum = vfs.getFileChecksum(
      new Path("/vfstmp/someFileForTestGetFileChecksum"));
    // Get checksum through HDFS. 
    FileChecksum hdfsCheckSum = fHdfs.getFileChecksum(
      new Path(someFile));
    // Get checksum of different file in HDFS
    FileChecksum otherHdfsFileCheckSum = fHdfs.getFileChecksum(
      new Path(someFile+"other"));
    // Checksums of the same file (got through HDFS and ViewFS should be same)
    assertEquals("HDFS and ViewFS checksums were not the same", viewFSCheckSum,
      hdfsCheckSum);
    // Checksum of different files should be different.
    assertFalse("Some other HDFS file which should not have had the same " +
      "checksum as viewFS did!", viewFSCheckSum.equals(otherHdfsFileCheckSum));
  }

  @AfterClass
  public static void cleanup() throws IOException {
    fHdfs.delete(new Path(testfilename), true);
    fHdfs.delete(new Path(someFile), true);
    fHdfs.delete(new Path(someFile + "other"), true);
  }

}
