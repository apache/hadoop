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
import static org.apache.hadoop.test.MockitoMaker.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class TestDataDirs {

  @Test public void testGetDataDirsFromURIs() throws Throwable {
    File localDir = make(stub(File.class).returning(true).from.exists());
    when(localDir.mkdir()).thenReturn(true);
    FsPermission normalPerm = new FsPermission("755");
    FsPermission badPerm = new FsPermission("000");
    FileStatus stat = make(stub(FileStatus.class)
        .returning(normalPerm, normalPerm, badPerm).from.getPermission());
    when(stat.isDirectory()).thenReturn(true);
    LocalFileSystem fs = make(stub(LocalFileSystem.class)
        .returning(stat).from.getFileStatus(any(Path.class)));
    when(fs.pathToFile(any(Path.class))).thenReturn(localDir);
    Collection<URI> uris = Arrays.asList(new URI("file:/p1/"),
        new URI("file:/p2/"), new URI("file:/p3/"));

    List<File> dirs = DataNode.getDataDirsFromURIs(uris, fs, normalPerm);

    verify(fs, times(2)).setPermission(any(Path.class), eq(normalPerm));
    verify(fs, times(6)).getFileStatus(any(Path.class));
    assertEquals("number of valid data dirs", dirs.size(), 1);
  }
}
