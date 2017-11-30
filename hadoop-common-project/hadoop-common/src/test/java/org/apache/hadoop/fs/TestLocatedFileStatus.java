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

package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * This class tests the LocatedFileStatus class.
 */
public class TestLocatedFileStatus {
  @Test
  public void testDeprecatedConstruction() throws IOException {
    BlockLocation[] locs = new BlockLocation[] {mock(BlockLocation.class)};
    final boolean isDir = false;
    final int repl = 3;
    final long blocksize = 64 * 1024 * 1024;
    final long modificationTime = 0;
    final long accessTime = 0;

    // We should be able to pass null for the permission.
    LocatedFileStatus lfsNullPerm = new LocatedFileStatus(1, isDir,
        repl, blocksize, modificationTime, accessTime, null, null, null,
        null, new Path("/some-file.txt"), locs);
    FsPermission permission = mock(FsPermission.class);

    // We should also be able to pass a permission or the permission.
    LocatedFileStatus lfsNonNullPerm = new LocatedFileStatus(1, isDir,
        repl, blocksize, modificationTime, accessTime, permission, null,
        null, null, new Path("/some-file.txt"), locs);
  }
}
