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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * A mock Azure Blob File System Authorization Implementation
 */
public class MockAbfsAuthorizer implements AbfsAuthorizer {

  public static final String TEST_READ_ONLY_FILE_0 = "readOnlyFile0";
  public static final String TEST_READ_ONLY_FILE_1 = "readOnlyFile1";
  public static final String TEST_READ_ONLY_FOLDER = "readOnlyFolder";
  public static final String TEST_WRITE_ONLY_FILE_0 = "writeOnlyFile0";
  public static final String TEST_WRITE_ONLY_FILE_1 = "writeOnlyFile1";
  public static final String TEST_WRITE_ONLY_FOLDER = "writeOnlyFolder";
  public static final String TEST_READ_WRITE_FILE_0 = "readWriteFile0";
  public static final String TEST_READ_WRITE_FILE_1 = "readWriteFile1";
  public static final String TEST_WRITE_THEN_READ_ONLY = "writeThenReadOnlyFile";
  private Configuration conf;
  private Set<Path> readOnlyPaths = new HashSet<Path>();
  private Set<Path> writeOnlyPaths = new HashSet<Path>();
  private Set<Path> readWritePaths = new HashSet<Path>();
  private int writeThenReadOnly = 0;
  public MockAbfsAuthorizer(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init() throws AbfsAuthorizationException, IOException {
    readOnlyPaths.add(new Path(TEST_READ_ONLY_FILE_0));
    readOnlyPaths.add(new Path(TEST_READ_ONLY_FILE_1));
    readOnlyPaths.add(new Path(TEST_READ_ONLY_FOLDER));
    writeOnlyPaths.add(new Path(TEST_WRITE_ONLY_FILE_0));
    writeOnlyPaths.add(new Path(TEST_WRITE_ONLY_FILE_1));
    writeOnlyPaths.add(new Path(TEST_WRITE_ONLY_FOLDER));
    readWritePaths.add(new Path(TEST_READ_WRITE_FILE_0));
    readWritePaths.add(new Path(TEST_READ_WRITE_FILE_1));
  }

  @Override
  public boolean isAuthorized(FsAction action, Path... absolutePaths) throws AbfsAuthorizationException, IOException {
    Set<Path> paths = new HashSet<Path>();
    for (Path path : absolutePaths) {
      paths.add(new Path(path.getName()));
    }

    if (action.equals(FsAction.READ) && Stream.concat(readOnlyPaths.stream(), readWritePaths.stream()).collect(Collectors.toSet()).containsAll(paths)) {
      return true;
    } else if (action.equals(FsAction.READ) && paths.contains(new Path(TEST_WRITE_THEN_READ_ONLY)) && writeThenReadOnly == 1) {
      return true;
    } else if (action.equals(FsAction.WRITE)
        && Stream.concat(writeOnlyPaths.stream(), readWritePaths.stream()).collect(Collectors.toSet()).containsAll(paths)) {
      return true;
    } else if (action.equals(FsAction.WRITE) && paths.contains(new Path(TEST_WRITE_THEN_READ_ONLY)) && writeThenReadOnly == 0) {
      writeThenReadOnly = 1;
      return true;
    } else {
      return action.equals(FsAction.READ_WRITE) && readWritePaths.containsAll(paths);
    }
  }
}
