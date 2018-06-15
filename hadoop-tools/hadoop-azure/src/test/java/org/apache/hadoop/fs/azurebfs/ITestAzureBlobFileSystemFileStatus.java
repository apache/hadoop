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

package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assert.assertEquals;

/**
 * Test FileStatus.
 */
public class ITestAzureBlobFileSystemFileStatus extends DependencyInjectedTest {
  private static final Path TEST_FILE = new Path("testFile");
  private static final Path TEST_FOLDER = new Path("testDir");
  public ITestAzureBlobFileSystemFileStatus() {
    super();
  }

  @Test
  public void testEnsureStatusWorksForRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    fs.getFileStatus(new Path("/"));
    fs.listStatus(new Path("/"));
  }

  @Test
  public void testFileStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(TEST_FILE);
    fs.mkdirs(TEST_FOLDER);

    FileStatus fileStatus = fs.getFileStatus(TEST_FILE);
    assertEquals(new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL), fileStatus.getPermission());
    assertEquals(fs.getOwnerUser(), fileStatus.getGroup());
    assertEquals(fs.getOwnerUserPrimaryGroup(), fileStatus.getOwner());

    fileStatus = fs.getFileStatus(TEST_FOLDER);
    assertEquals(new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL), fileStatus.getPermission());
    assertEquals(fs.getOwnerUser(), fileStatus.getGroup());
    assertEquals(fs.getOwnerUserPrimaryGroup(), fileStatus.getOwner());
  }
}
