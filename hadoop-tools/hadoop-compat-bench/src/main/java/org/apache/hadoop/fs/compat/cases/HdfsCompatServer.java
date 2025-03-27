/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.cases;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.compat.common.*;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@HdfsCompatCaseGroup(name = "Server")
public class HdfsCompatServer extends AbstractHdfsCompatCase {
  private void isValid(String name) {
    Assert.assertNotNull(name);
    Assert.assertFalse(name.isEmpty());
  }

  @HdfsCompatCase
  public void initialize() throws Exception {
    Class<? extends FileSystem> cls = FileSystem.getFileSystemClass(
        getBasePath().toUri().getScheme(), fs().getConf());
    Constructor<? extends FileSystem> ctor =
        cls.getDeclaredConstructor();
    ctor.setAccessible(true);
    FileSystem newFs = ctor.newInstance();
    newFs.initialize(fs().getUri(), fs().getConf());
  }

  @HdfsCompatCase
  public void getScheme() {
    final String scheme = fs().getScheme();
    isValid(scheme);
  }

  @HdfsCompatCase
  public void getUri() {
    URI uri = fs().getUri();
    isValid(uri.getScheme());
  }

  @HdfsCompatCase
  public void getCanonicalServiceName() {
    final String serviceName = fs().getCanonicalServiceName();
    isValid(serviceName);
  }

  @HdfsCompatCase
  public void getName() {
    final String name = fs().getName();
    isValid(name);
  }

  @HdfsCompatCase
  public void makeQualified() {
    Path path = fs().makeQualified(makePath("file"));
    isValid(path.toUri().getScheme());
  }

  @HdfsCompatCase
  public void getChildFileSystems() {
    fs().getChildFileSystems();
  }

  @HdfsCompatCase
  public void resolvePath() throws IOException {
    FileSystem.enableSymlinks();
    Path file = makePath("file");
    Path link = new Path(file.toString() + "_link");
    HdfsCompatUtil.createFile(fs(), file, 0);
    fs().createSymlink(file, link, true);
    Path resolved = fs().resolvePath(link);
    Assert.assertEquals(file.getName(), resolved.getName());
  }

  @HdfsCompatCase
  public void getHomeDirectory() {
    final Path home = fs().getHomeDirectory();
    isValid(home.toString());
  }

  @HdfsCompatCase
  public void setWorkingDirectory() throws IOException {
    FileSystem another = FileSystem.newInstance(fs().getUri(), fs().getConf());
    Path work = makePath("work");
    another.setWorkingDirectory(work);
    Assert.assertEquals(work.getName(),
        another.getWorkingDirectory().getName());
  }

  @HdfsCompatCase
  public void getWorkingDirectory() {
    Path work = fs().getWorkingDirectory();
    isValid(work.toString());
  }

  @HdfsCompatCase
  public void close() throws IOException {
    FileSystem another = FileSystem.newInstance(fs().getUri(), fs().getConf());
    another.close();
  }

  @HdfsCompatCase
  public void getDefaultBlockSize() {
    Assert.assertTrue(fs().getDefaultBlockSize(getBasePath()) >= 0);
  }

  @HdfsCompatCase
  public void getDefaultReplication() {
    Assert.assertTrue(fs().getDefaultReplication(getBasePath()) >= 0);
  }

  @HdfsCompatCase
  public void getStorageStatistics() {
    Assert.assertNotNull(fs().getStorageStatistics());
  }

  // @HdfsCompatCase
  public void setVerifyChecksum() {
  }

  // @HdfsCompatCase
  public void setWriteChecksum() {
  }

  @HdfsCompatCase
  public void getDelegationToken() throws IOException {
    Assert.assertNotNull(fs().getDelegationToken(getDelegationTokenRenewer()));
  }

  @HdfsCompatCase
  public void getAdditionalTokenIssuers() throws IOException {
    Assert.assertNotNull(fs().getAdditionalTokenIssuers());
  }

  @HdfsCompatCase
  public void getServerDefaults() throws IOException {
    FsServerDefaults d = fs().getServerDefaults(getBasePath());
    Assert.assertTrue(d.getBlockSize() >= 0);
  }

  @HdfsCompatCase
  public void msync() throws IOException {
    fs().msync();
  }

  @HdfsCompatCase
  public void getStatus() throws IOException {
    FsStatus status = fs().getStatus();
    Assert.assertTrue(status.getRemaining() > 0);
  }

  @HdfsCompatCase
  public void getTrashRoot() {
    Path trash = fs().getTrashRoot(makePath("file"));
    isValid(trash.toString());
  }

  @HdfsCompatCase
  public void getTrashRoots() {
    Collection<FileStatus> trashes = fs().getTrashRoots(true);
    Assert.assertNotNull(trashes);
    for (FileStatus trash : trashes) {
      isValid(trash.getPath().toString());
    }
  }

  @HdfsCompatCase
  public void getAllStoragePolicies() throws IOException {
    Collection<? extends BlockStoragePolicySpi> policies =
        fs().getAllStoragePolicies();
    Assert.assertFalse(policies.isEmpty());
  }

  @HdfsCompatCase
  public void supportsSymlinks() {
    Assert.assertTrue(fs().supportsSymlinks());
  }

  @HdfsCompatCase
  public void hasPathCapability() throws IOException {
    List<String> allCaps = new ArrayList<>();
    allCaps.add(CommonPathCapabilities.FS_ACLS);
    allCaps.add(CommonPathCapabilities.FS_APPEND);
    allCaps.add(CommonPathCapabilities.FS_CHECKSUMS);
    allCaps.add(CommonPathCapabilities.FS_CONCAT);
    allCaps.add(CommonPathCapabilities.FS_LIST_CORRUPT_FILE_BLOCKS);
    allCaps.add(CommonPathCapabilities.FS_PATHHANDLES);
    allCaps.add(CommonPathCapabilities.FS_PERMISSIONS);
    allCaps.add(CommonPathCapabilities.FS_READ_ONLY_CONNECTOR);
    allCaps.add(CommonPathCapabilities.FS_SNAPSHOTS);
    allCaps.add(CommonPathCapabilities.FS_STORAGEPOLICY);
    allCaps.add(CommonPathCapabilities.FS_SYMLINKS);
    allCaps.add(CommonPathCapabilities.FS_TRUNCATE);
    allCaps.add(CommonPathCapabilities.FS_XATTRS);
    final Path base = getBasePath();
    for (String cap : allCaps) {
      if (fs().hasPathCapability(base, cap)) {
        return;
      }
    }
    throw new IOException("Cannot find any path capability");
  }
}