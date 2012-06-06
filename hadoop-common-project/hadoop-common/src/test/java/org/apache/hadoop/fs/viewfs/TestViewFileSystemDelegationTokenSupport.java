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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.junit.Test;

/**
 * Test ViewFileSystem's support for having delegation tokens fetched and cached
 * for the file system.
 * 
 * Currently this class just ensures that getCanonicalServiceName() always
 * returns <code>null</code> for ViewFileSystem instances.
 */
public class TestViewFileSystemDelegationTokenSupport {
  
  private static final String MOUNT_TABLE_NAME = "vfs-cluster";

  /**
   * Regression test for HADOOP-8408.
   */
  @Test
  public void testGetCanonicalServiceNameWithNonDefaultMountTable()
      throws URISyntaxException, IOException {
    
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, MOUNT_TABLE_NAME, "/user", new URI("file:///"));
    
    FileSystem viewFs = FileSystem.get(new URI(FsConstants.VIEWFS_SCHEME +
        "://" + MOUNT_TABLE_NAME), conf);
    
    String serviceName = viewFs.getCanonicalServiceName();
    assertNull(serviceName);
  }
  
  @Test
  public void testGetCanonicalServiceNameWithDefaultMountTable()
      throws URISyntaxException, IOException {
    
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/user", new URI("file:///"));
    
    FileSystem viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    
    String serviceName = viewFs.getCanonicalServiceName();
    assertNull(serviceName);
  }

}
