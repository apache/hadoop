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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.compat.common.*;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@HdfsCompatCaseGroup(name = "FileSystem")
public class HdfsCompatBasics extends AbstractHdfsCompatCase {
  @HdfsCompatCase
  public void initialize() throws IOException {
    FileSystem another = FileSystem.newInstance(fs().getUri(), fs().getConf());
    HdfsCompatUtil.checkImplementation(() ->
        another.initialize(URI.create("hdfs:///"), new Configuration())
    );
  }

  @HdfsCompatCase
  public void getScheme() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getScheme()
    );
  }

  @HdfsCompatCase
  public void getUri() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getUri()
    );
  }

  @HdfsCompatCase
  public void getCanonicalServiceName() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getCanonicalServiceName()
    );
  }

  @HdfsCompatCase
  public void getName() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getName()
    );
  }

  @HdfsCompatCase
  public void makeQualified() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().makeQualified(new Path("/"))
    );
  }

  @HdfsCompatCase
  public void getChildFileSystems() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getChildFileSystems()
    );
  }

  @HdfsCompatCase
  public void resolvePath() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().resolvePath(new Path("/"))
    );
  }

  @HdfsCompatCase
  public void getHomeDirectory() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getHomeDirectory()
    );
  }

  @HdfsCompatCase
  public void setWorkingDirectory() throws IOException {
    FileSystem another = FileSystem.newInstance(fs().getUri(), fs().getConf());
    HdfsCompatUtil.checkImplementation(() ->
        another.setWorkingDirectory(makePath("/tmp"))
    );
  }

  @HdfsCompatCase
  public void getWorkingDirectory() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getWorkingDirectory()
    );
  }

  @HdfsCompatCase
  public void close() throws IOException {
    FileSystem another = FileSystem.newInstance(fs().getUri(), fs().getConf());
    HdfsCompatUtil.checkImplementation(another::close);
  }

  @HdfsCompatCase
  public void getDefaultBlockSize() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getDefaultBlockSize(getBasePath())
    );
  }

  @HdfsCompatCase
  public void getDefaultReplication() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getDefaultReplication(getBasePath())
    );
  }

  @HdfsCompatCase
  public void getStorageStatistics() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getStorageStatistics()
    );
  }

  @HdfsCompatCase
  public void setVerifyChecksum() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setVerifyChecksum(true)
    );
  }

  @HdfsCompatCase
  public void setWriteChecksum() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setWriteChecksum(true)
    );
  }

  @HdfsCompatCase
  public void getDelegationToken() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getDelegationToken("hadoop")
    );
  }

  @HdfsCompatCase
  public void getAdditionalTokenIssuers() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getAdditionalTokenIssuers()
    );
  }

  @HdfsCompatCase
  public void getServerDefaults() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getServerDefaults(new Path("/"))
    );
  }

  @HdfsCompatCase
  public void msync() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().msync()
    );
  }

  @HdfsCompatCase
  public void getStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getStatus(new Path("/"))
    );
  }

  @HdfsCompatCase
  public void getTrashRoot() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getTrashRoot(new Path("/user/hadoop/tmp"))
    );
  }

  @HdfsCompatCase
  public void getTrashRoots() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getTrashRoots(true)
    );
  }

  @HdfsCompatCase
  public void getAllStoragePolicies() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getAllStoragePolicies()
    );
  }

  @HdfsCompatCase
  public void supportsSymlinks() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().supportsSymlinks()
    );
  }

  @HdfsCompatCase
  public void hasPathCapability() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().hasPathCapability(getBasePath(),
            CommonPathCapabilities.FS_TRUNCATE)
    );
  }

  @HdfsCompatCase
  public void mkdirs() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().mkdirs(makePath("mkdir"))
    );
  }

  @HdfsCompatCase
  public void getFileStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getFileStatus(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void exists() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().exists(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void isDirectory() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().isDirectory(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void isFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().isFile(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getLength() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getLength(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getBlockSize() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getBlockSize(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void listStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listStatus(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void globStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().globStatus(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void listLocatedStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listLocatedStatus(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void listStatusIterator() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listStatusIterator(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void listFiles() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listFiles(makePath("dir"), false)
    );
  }

  @HdfsCompatCase
  public void rename() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().rename(makePath("src"), makePath("dst"))
    );
  }

  @HdfsCompatCase
  public void delete() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().delete(makePath("file"), true)
    );
  }

  @HdfsCompatCase
  public void deleteOnExit() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().deleteOnExit(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void cancelDeleteOnExit() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().cancelDeleteOnExit(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void truncate() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().truncate(makePath("file"), 1)
    );
  }

  @HdfsCompatCase
  public void setOwner() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setOwner(makePath("file"), "test-user", "test-group")
    );
  }

  @HdfsCompatCase
  public void setTimes() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setTimes(makePath("file"), 1696089600L, 1696089600L)
    );
  }

  @HdfsCompatCase
  public void concat() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().concat(makePath("file"),
            new Path[]{makePath("file1"), makePath("file2")})
    );
  }

  @HdfsCompatCase
  public void getFileChecksum() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getFileChecksum(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getFileBlockLocations() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getFileBlockLocations(new FileStatus(), 0, 128)
    );
  }

  @HdfsCompatCase
  public void listCorruptFileBlocks() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listCorruptFileBlocks(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getReplication() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getReplication(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void setReplication() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setReplication(makePath("file"), (short) 2)
    );
  }

  @HdfsCompatCase
  public void getPathHandle() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getPathHandle(new FileStatus())
    );
  }

  @HdfsCompatCase
  public void create() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().create(makePath("file"), true)
    );
  }

  @HdfsCompatCase
  public void createNonRecursive() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().createNonRecursive(makePath("file"), true, 1024,
            (short) 1, 1048576, null)
    );
  }

  @HdfsCompatCase
  public void createNewFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().createNewFile(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void append() throws IOException {
    final Path file = makePath("file");
    try {
      HdfsCompatUtil.createFile(fs(), file, 0);
      HdfsCompatUtil.checkImplementation(() ->
          fs().append(file)
      );
    } finally {
      HdfsCompatUtil.deleteQuietly(fs(), file, true);
    }
  }

  @HdfsCompatCase
  public void createFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().createFile(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void appendFile() throws IOException {
    final Path file = makePath("file");
    try {
      HdfsCompatUtil.createFile(fs(), file, 0);
      HdfsCompatUtil.checkImplementation(() ->
          fs().appendFile(file)
      );
    } finally {
      HdfsCompatUtil.deleteQuietly(fs(), file, true);
    }
  }

  @HdfsCompatCase
  public void createMultipartUploader() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().createMultipartUploader(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void open() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().open(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void openFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().openFile(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getContentSummary() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getContentSummary(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void getUsed() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getUsed(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void getQuotaUsage() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getQuotaUsage(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void setQuota() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setQuota(makePath("dir"), 1024L, 1048576L)
    );
  }

  @HdfsCompatCase
  public void setQuotaByStorageType() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setQuotaByStorageType(makePath("dir"), StorageType.SSD, 1048576L)
    );
  }

  @HdfsCompatCase
  public void access() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().access(makePath("file"), FsAction.EXECUTE)
    );
  }

  @HdfsCompatCase
  public void setPermission() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setPermission(makePath("file"), FsPermission.getDefault())
    );
  }

  @HdfsCompatCase
  public void createSymlink() {
    FileSystem.enableSymlinks();
    HdfsCompatUtil.checkImplementation(() ->
        fs().createSymlink(makePath("file"), makePath("link"), true)
    );
  }

  @HdfsCompatCase
  public void getFileLinkStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getFileLinkStatus(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void getLinkTarget() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getLinkTarget(makePath("link"))
    );
  }

  @HdfsCompatCase
  public void modifyAclEntries() {
    List<AclEntry> entries = AclEntry.parseAclSpec("user:foo:---", true);
    HdfsCompatUtil.checkImplementation(() ->
        fs().modifyAclEntries(makePath("modifyAclEntries"), entries)
    );
  }

  @HdfsCompatCase
  public void removeAclEntries() {
    List<AclEntry> entries = AclEntry.parseAclSpec("user:foo:---", true);
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeAclEntries(makePath("removeAclEntries"), entries)
    );
  }

  @HdfsCompatCase
  public void removeDefaultAcl() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeDefaultAcl(makePath("removeDefaultAcl"))
    );
  }

  @HdfsCompatCase
  public void removeAcl() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeAcl(makePath("removeAcl"))
    );
  }

  @HdfsCompatCase
  public void setAcl() {
    List<AclEntry> entries = AclEntry.parseAclSpec("user:foo:---", true);
    HdfsCompatUtil.checkImplementation(() ->
        fs().setAcl(makePath("setAcl"), entries)
    );
  }

  @HdfsCompatCase
  public void getAclStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getAclStatus(makePath("getAclStatus"))
    );
  }

  @HdfsCompatCase
  public void setXAttr() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setXAttr(makePath("file"), "test-xattr",
            "test-value".getBytes(StandardCharsets.UTF_8))
    );
  }

  @HdfsCompatCase
  public void getXAttr() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getXAttr(makePath("file"), "test-xattr")
    );
  }

  @HdfsCompatCase
  public void getXAttrs() {
    List<String> names = new ArrayList<>();
    names.add("test-xattr");
    HdfsCompatUtil.checkImplementation(() ->
        fs().getXAttrs(makePath("file"), names)
    );
  }

  @HdfsCompatCase
  public void listXAttrs() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().listXAttrs(makePath("file"))
    );
  }

  @HdfsCompatCase
  public void removeXAttr() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeXAttr(makePath("file"), "test-xattr")
    );
  }

  @HdfsCompatCase
  public void setStoragePolicy() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setStoragePolicy(makePath("dir"), "COLD")
    );
  }

  @HdfsCompatCase
  public void unsetStoragePolicy() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().unsetStoragePolicy(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void satisfyStoragePolicy() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().satisfyStoragePolicy(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void getStoragePolicy() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getStoragePolicy(makePath("dir"))
    );
  }

  @HdfsCompatCase
  public void copyFromLocalFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().copyFromLocalFile(makePath("src"), makePath("dst"))
    );
  }

  @HdfsCompatCase
  public void moveFromLocalFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().moveFromLocalFile(makePath("src"), makePath("dst"))
    );
  }

  @HdfsCompatCase
  public void copyToLocalFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().copyToLocalFile(makePath("src"), makePath("dst"))
    );
  }

  @HdfsCompatCase
  public void moveToLocalFile() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().moveToLocalFile(makePath("src"), makePath("dst"))
    );
  }

  @HdfsCompatCase
  public void startLocalOutput() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().startLocalOutput(makePath("out"), makePath("tmp"))
    );
  }

  @HdfsCompatCase
  public void completeLocalOutput() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().completeLocalOutput(makePath("out"), makePath("tmp"))
    );
  }

  @HdfsCompatCase
  public void createSnapshot() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().createSnapshot(makePath("file"), "s_name")
    );
  }

  @HdfsCompatCase
  public void renameSnapshot() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().renameSnapshot(makePath("file"), "s_name", "n_name")
    );
  }

  @HdfsCompatCase
  public void deleteSnapshot() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().deleteSnapshot(makePath("file"), "s_name")
    );
  }
}