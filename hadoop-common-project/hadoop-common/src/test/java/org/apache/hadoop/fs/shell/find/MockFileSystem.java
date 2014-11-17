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
package org.apache.hadoop.fs.shell.find;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A mock {@link FileSystem} for use with the {@link Find} unit tests. Usage:
 * FileSystem mockFs = MockFileSystem.setup(); Methods in the mockFs can then be
 * mocked out by the test script. The {@link Configuration} can be accessed by
 * mockFs.getConf(); The following methods are fixed within the class: -
 * {@link FileSystem#initialize(URI,Configuration)} blank stub -
 * {@link FileSystem#makeQualified(Path)} returns the passed in {@link Path} -
 * {@link FileSystem#getWorkingDirectory} returns new Path("/") -
 * {@link FileSystem#resolvePath(Path)} returns the passed in {@link Path}
 */
class MockFileSystem extends FilterFileSystem {
  private static FileSystem mockFs = null;

  /** Setup and return the underlying {@link FileSystem} mock */
  static FileSystem setup() throws IOException {
    if (mockFs == null) {
      mockFs = mock(FileSystem.class);
    }
    reset(mockFs);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "mockfs:///");
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    when(mockFs.getConf()).thenReturn(conf);
    return mockFs;
  }

  private MockFileSystem() {
    super(mockFs);
  }

  @Override
  public void initialize(URI uri, Configuration conf) {
  }

  @Override
  public Path makeQualified(Path path) {
    return path;
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return fs.globStatus(pathPattern);
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/");
  }

  @Override
  public Path resolvePath(final Path p) throws IOException {
    return p;
  }
}
