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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final Configuration conf = new Configuration();
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  static {
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      cluster.waitActive();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setUp() throws Exception {
    final String uri = WebHdfsFileSystem.SCHEME  + "://"
        + conf.get("dfs.http.address");
    fs = FileSystem.get(new URI(uri), conf); 
    defaultWorkingDirectory = fs.getWorkingDirectory().toUri().getPath();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }


  //In trunk, testListStatusReturnsNullForNonExistentFile was replaced by
  //testListStatusThrowsExceptionForNonExistentFile.
  //
  //For WebHdfsFileSystem,
  //disable testListStatusReturnsNullForNonExistentFile
  //and add testListStatusThrowsExceptionForNonExistentFile below.
  public void testListStatusReturnsNullForNonExistentFile() {}

  public void testListStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      fs.listStatus(path("/test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
}
