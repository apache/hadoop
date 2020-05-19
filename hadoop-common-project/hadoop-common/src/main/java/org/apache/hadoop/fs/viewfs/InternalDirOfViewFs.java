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

import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;
import static org.apache.hadoop.fs.viewfs.Constants.ROOT_PATH;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.viewfs.Constants.*;


/**
 * An instance of this class represents an internal dir of the viewFs
 * that is internal dir of the mount table.
 * It is a read only mount tables and create, mkdir or delete operations
 * are not allowed.
 * If called on create or mkdir then this target is the parent of the
 * directory in which one is trying to create or mkdir; hence
 * in this case the path name passed in is the last component. 
 * Otherwise this target is the end point of the path and hence
 * the path name passed in is null. 
 */
public class InternalDirOfViewFs extends FileSystem {
  final InodeTree.INodeDir<FileSystem>  theInternalDir;
  final long creationTime; // of the the mount table
  final UserGroupInformation ugi; // the user/group of user who created mtable
  final URI myUri;

  public InternalDirOfViewFs(final InodeTree.INodeDir<FileSystem> dir,
      final long cTime, final UserGroupInformation ugi, URI uri,
      Configuration config) throws URISyntaxException {
    myUri = uri;
    try {
      initialize(myUri, config);
    } catch (IOException e) {
      throw new RuntimeException("Cannot occur");
    }
    theInternalDir = dir;
    creationTime = cTime;
    this.ugi = ugi;
  }

  static private void checkPathIsSlash(final Path f) throws IOException {
    if (f != InodeTree.SlashPath) {
      throw new IOException(
          "Internal implementation error: expected file name to be /");
    }
  }

  @Override
  public URI getUri() {
    return myUri;
  }

  @Override
  public Path getWorkingDirectory() {
    throw new RuntimeException(
        "Internal impl error: getWorkingDir should not have been called");
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    throw new RuntimeException(
        "Internal impl error: getWorkingDir should not have been called");
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    throw ViewFileSystemUtil.readOnlyMountTable("append", f);
  }

  @Override
  public FSDataOutputStream create(final Path f,
      final FsPermission permission, final boolean overwrite,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws AccessControlException {
    throw ViewFileSystemUtil.readOnlyMountTable("create", f);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, IOException {
    checkPathIsSlash(f);
    throw ViewFileSystemUtil.readOnlyMountTable("delete", f);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(final Path f)
      throws AccessControlException, IOException {
    return delete(f, true);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus fs,
      final long start, final long len) throws FileNotFoundException, IOException {
    checkPathIsSlash(fs.getPath());
    throw new FileNotFoundException("Path points to dir not a file");
  }

  @Override
  public FileChecksum getFileChecksum(final Path f)
      throws FileNotFoundException, IOException {
    checkPathIsSlash(f);
    throw new FileNotFoundException("Path points to dir not a file");
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    checkPathIsSlash(f);
    return new FileStatus(0, true, 0, 0, creationTime, creationTime,
        PERMISSION_555, ugi.getShortUserName(), ugi.getPrimaryGroupName(),

        new Path(theInternalDir.fullPath).makeQualified(
            myUri, ROOT_PATH));
  }


  @Override
  public FileStatus[] listStatus(Path f) throws AccessControlException,
                                                FileNotFoundException, IOException {
    checkPathIsSlash(f);
    FileStatus[] result = new FileStatus[theInternalDir.getChildren().size()];
    int i = 0;
    for (Map.Entry<String, InodeTree.INode<FileSystem>> iEntry :
        theInternalDir.getChildren().entrySet()) {
      InodeTree.INode<FileSystem> inode = iEntry.getValue();
      if (inode.isLink()) {
        InodeTree.INodeLink<FileSystem> link = (InodeTree.INodeLink<FileSystem>) inode;

        result[i++] = new FileStatus(0, false, 0, 0,
            creationTime, creationTime, PERMISSION_555,
            ugi.getShortUserName(), ugi.getPrimaryGroupName(),
            link.getTargetLink(),
            new Path(inode.fullPath).makeQualified(
                myUri, null));
      } else {
        result[i++] = new FileStatus(0, true, 0, 0,
            creationTime, creationTime, PERMISSION_555,
            ugi.getShortUserName(), ugi.getGroupNames()[0],
            new Path(inode.fullPath).makeQualified(
                myUri, null));
      }
    }
    return result;
  }

  @Override
  public boolean mkdirs(Path dir, FsPermission permission)
      throws AccessControlException, FileAlreadyExistsException {
    if (theInternalDir.isRoot() && dir == null) {
      throw new FileAlreadyExistsException("/ already exits");
    }
    // Note dir starts with /
    if (theInternalDir.getChildren().containsKey(
        dir.toString().substring(1))) {
      return true; // this is the stupid semantics of FileSystem
    }
    throw ViewFileSystemUtil.readOnlyMountTable("mkdirs",  dir);
  }

  @Override
  public boolean mkdirs(Path dir)
      throws AccessControlException, FileAlreadyExistsException {
    return mkdirs(dir, null);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws AccessControlException, FileNotFoundException, IOException {
    checkPathIsSlash(f);
    throw new FileNotFoundException("Path points to dir not a file");
  }

  @Override
  public boolean rename(Path src, Path dst) throws AccessControlException,
                                                   IOException {
    checkPathIsSlash(src);
    checkPathIsSlash(dst);
    throw ViewFileSystemUtil.readOnlyMountTable("rename", src);
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    throw ViewFileSystemUtil.readOnlyMountTable("truncate", f);
  }

  @Override
  public void setOwner(Path f, String username, String groupname)
      throws AccessControlException, IOException {
    checkPathIsSlash(f);
    throw ViewFileSystemUtil.readOnlyMountTable("setOwner", f);
  }

  @Override
  public void setPermission(Path f, FsPermission permission)
      throws AccessControlException, IOException {
    checkPathIsSlash(f);
    throw ViewFileSystemUtil.readOnlyMountTable("setPermission", f);
  }

  @Override
  public boolean setReplication(Path f, short replication)
      throws AccessControlException, IOException {
    checkPathIsSlash(f);
    throw ViewFileSystemUtil.readOnlyMountTable("setReplication", f);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime)
      throws AccessControlException, IOException {
    checkPathIsSlash(f);
    throw ViewFileSystemUtil.readOnlyMountTable("setTimes", f);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    // Noop for viewfs
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    throw new NotInMountpointException(f, "getServerDefaults");
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    throw new NotInMountpointException(f, "getDefaultBlockSize");
  }

  @Override
  public short getDefaultReplication(Path f) {
    throw new NotInMountpointException(f, "getDefaultReplication");
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("modifyAclEntries", path);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("removeAclEntries", path);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("removeDefaultAcl", path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("removeAcl", path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("setAcl", path);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    checkPathIsSlash(path);
    return new AclStatus.Builder().owner(ugi.getShortUserName())
        .group(ugi.getPrimaryGroupName())
        .addEntries(AclUtil.getMinimalAcl(PERMISSION_555))
        .stickyBit(false).build();
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("setXAttr", path);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    throw new NotInMountpointException(path, "getXAttr");
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    throw new NotInMountpointException(path, "getXAttrs");
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    throw new NotInMountpointException(path, "getXAttrs");
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    throw new NotInMountpointException(path, "listXAttrs");
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("removeXAttr", path);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("createSnapshot", path);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("renameSnapshot", path);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    checkPathIsSlash(path);
    throw ViewFileSystemUtil.readOnlyMountTable("deleteSnapshot", path);
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    throw new NotInMountpointException(f, "getQuotaUsage");
  }

  @Override
  public void satisfyStoragePolicy(Path src) throws IOException {
    checkPathIsSlash(src);
    throw ViewFileSystemUtil.readOnlyMountTable("satisfyStoragePolicy", src);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName)
      throws IOException {
    checkPathIsSlash(src);
    throw ViewFileSystemUtil.readOnlyMountTable("setStoragePolicy", src);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    checkPathIsSlash(src);
    throw ViewFileSystemUtil.readOnlyMountTable("unsetStoragePolicy", src);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    throw new NotInMountpointException(src, "getStoragePolicy");
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    Collection<BlockStoragePolicySpi> allPolicies = new HashSet<>();
    for (FileSystem fs : getChildFileSystems()) {
      try {
        Collection<? extends BlockStoragePolicySpi> policies =
            fs.getAllStoragePolicies();
        allPolicies.addAll(policies);
      } catch (UnsupportedOperationException e) {
        // ignored
      }
    }
    return allPolicies;
  }
}
