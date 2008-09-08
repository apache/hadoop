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

package org.apache.hadoop.fs.s3native;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

public abstract class NativeS3FileSystemContractBaseTest
  extends FileSystemContractBaseTest {
  
  private NativeFileSystemStore store;
  
  abstract NativeFileSystemStore getNativeFileSystemStore() throws IOException;

  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    store = getNativeFileSystemStore();
    fs = new NativeS3FileSystem(store);
    fs.initialize(URI.create(conf.get("test.fs.s3n.name")), conf);
  }
  
  @Override
  protected void tearDown() throws Exception {
    store.purge("test");
    super.tearDown();
  }
  
  public void testListStatusForRoot() throws Exception {
    Path testDir = path("/test");
    assertTrue(fs.mkdirs(testDir));
    
    FileStatus[] paths = fs.listStatus(path("/"));
    assertEquals(1, paths.length);
    assertEquals(path("/test"), paths[0].getPath());
  }
  
}
