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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode.DataNodeDiskChecker;

public class TestDataDirs {

  @Test (timeout = 10000)
  public void testGetDataDirsFromURIs() throws Throwable {
    
    DataNodeDiskChecker diskChecker = mock(DataNodeDiskChecker.class);
    doThrow(new IOException()).doThrow(new IOException()).doNothing()
      .when(diskChecker).checkDir(any(LocalFileSystem.class), any(Path.class));
    LocalFileSystem fs = mock(LocalFileSystem.class);
    Collection<URI> uris = Arrays.asList(new URI("file:/p1/"),
        new URI("file:/p2/"), new URI("file:/p3/"));

    List<File> dirs = DataNode.getDataDirsFromURIs(uris, fs, diskChecker);
    assertEquals("number of valid data dirs", 1, dirs.size());
    String validDir = dirs.iterator().next().getPath();
    assertEquals("p3 should be valid", new File("/p3").getPath(), validDir);
  }
}
