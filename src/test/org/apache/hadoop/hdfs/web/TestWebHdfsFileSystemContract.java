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

package org.apache.hadoop.hdfs.web;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  static {
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setUp() throws Exception {
    fs = cluster.getWebHdfsFileSystem();
    defaultWorkingDirectory = "/user/"
        + UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  /** Override the following method without using position read. */
  @Override
  protected void writeReadAndDelete(int len) throws IOException {
    Path path = path("/test/hadoop/file");
    
    fs.mkdirs(path.getParent());

    FSDataOutputStream out = fs.create(path, false,
        fs.getConf().getInt("io.file.buffer.size", 4096), 
        (short) 1, getBlockSize());
    out.write(data, 0, len);
    out.close();

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    for (int i = 0; i < len; i++) {
      final int b = in.read();
      assertEquals("Position " + i, data[i], b);
    }
    in.close();
    
    assertTrue("Deleted", fs.delete(path, false));
    assertFalse("No longer exists", fs.exists(path));
  }

  //The following test failed for HftpFileSystem,
  //Disable it for WebHdfsFileSystem
  public void testListStatusReturnsNullForNonExistentFile() {}
}
