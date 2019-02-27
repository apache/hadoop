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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChRootedFileSystem</code> is a file system with its root some path
 * below the root of its base file system. 
 * 
 * Example: For a base file system hdfs://nn1/ with chRoot at /usr/foo, the
 * members will be setup as shown below.
 * <ul>
 * <li>myFs is the base file system and points to hdfs at nn1</li>
 * <li>myURI is hdfs://nn1/user/foo</li>
 * <li>chRootPathPart is /user/foo</li>
 * <li>workingDir is a directory related to chRoot</li>
 * </ul>
 * 
 * The paths are resolved as follows by ChRootedFileSystem:
 * <ul>
 * <li> Absolute path /a/b/c is resolved to /user/foo/a/b/c at myFs</li>
 * <li> Relative path x/y is resolved to /user/foo/<workingDir>/x/y</li>
 * </ul>
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFileSystem extends FilterFileSystem {
  private final URI myUri; // the base URI + the chRoot
  private final Path chRootPathPart; // the root below the root of the base
  private final String chRootPathPartString;
  private Path workingDir;
  
  protected FileSystem getMyFs() {
    return getRawFileSystem();
  }
  
  /**
   * @param path
   * @return  full path including the chroot 
   */
  protected Path fullPath(final Path path) {
    super.checkPath(path);
    return path.isAbsolute() ? 
        new Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
            + path.toUri().getPath()) :
        new Path(chRootPathPartString + workingDir.toUri().getPath(), path);
  }
  
  /**
   * Constructor
   * @param uri base file system
   * @param conf configuration
   * @throws IOException 
   */
  public ChRootedFileSystem(final URI uri, Configuration conf)
      throws IOException {
    super(FileSystem.get(uri, conf));
    String pathString = uri.getPath();
    if (pathString.isEmpty()) {
      pathString = "/";
    }
    chRootPathPart = new Path(pathString);
    chRootPathPartString = chRootPathPart.toUri().getPath();
    myUri = uri;
    workingDir = getHomeDirectory();
    // We don't use the wd of the myFs
  }
  
  /** 
   * Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(final URI name, final Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    setConf(conf);
  }

  @Override
  public URI getUri() {
    return myUri;
  }
  
  /**
   * Strip out the root from the path.
   * @param p - fully qualified path p
   * @return -  the remaining path  without the beginning /
   * @throws IOException if the p is not prefixed with root
   */
  String stripOutRoot(final Path p) throws IOException {
    try {
     checkPath(p);
    } catch (IllegalArgumentException e) {
      throw new IOException("Internal Error - path " + p +
          " should have been with URI: " + myUri);
    }
    String pathPart = p.toUri().getPath();
    return (pathPart.length() == chRootPathPartString.length()) ? "" : pathPart
        .substring(chRootPathPartString.length() + (chRootPathPart.isRoot() ? 0 : 1));
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    /*
     * 3 choices here: 
     *     null or / or /user/<uname> or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     * so that the default rule for wd is applied
     */
    return null;
  }
  
  public Path getResolvedQualifiedPath(final Path f)
      throws FileNotFoundException {
    return makeQualified(
        new Path(chRootPathPartString + f.toUri().toString()));
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  public void setWorkingDirectory(final Path new_dir) {
    workingDir = new_dir.isAbsolute() ? new_dir : new Path(workingDir, new_dir);
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    return super.create(fullPath(f), permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }
  
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    return super.createNonRecursive(fullPath(f), permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) 
      throws IOException {
    return super.delete(fullPath(f), recursive);
  }
  

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(Path f) throws IOException {
   return delete(f, true);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus fs, final long start,
      final long len) throws IOException {
    return super.getFileBlockLocations(
        new ViewFsFileStatus(fs, fullPath(fs.getPath())), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f) 
      throws IOException {
    return super.getFileChecksum(fullPath(f));
  }

  @Override
  public FileChecksum getFileChecksum(final Path f, final long length)
      throws IOException {
    return super.getFileChecksum(fullPath(f), length);
  }

  @Override
  public FileStatus getFileStatus(final Path f) 
      throws IOException {
    return super.getFileStatus(fullPath(f));
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    return super.getLinkTarget(fullPath(f));
  }

  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, IOException {
    super.access(fullPath(path), mode);
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return super.getStatus(fullPath(p));
  }

  @Override
  public FileStatus[] listStatus(final Path f) 
      throws IOException {
    return super.listStatus(fullPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws IOException {
    return super.listLocatedStatus(fullPath(f));
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission)
      throws IOException {
    return super.mkdirs(fullPath(f), permission);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize) 
    throws IOException {
    return super.open(fullPath(f), bufferSize);
  }
  
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return super.append(fullPath(f), bufferSize, progress);
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    return super.rename(fullPath(src), fullPath(dst)); 
  }
  
  @Override
  public void setOwner(final Path f, final String username,
      final String groupname)
    throws IOException {
    super.setOwner(fullPath(f), username, groupname);
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
    throws IOException {
    super.setPermission(fullPath(f), permission);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
    throws IOException {
    return super.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime) 
      throws IOException {
    super.setTimes(fullPath(f), mtime, atime);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    super.modifyAclEntries(fullPath(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    super.removeAclEntries(fullPath(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    super.removeDefaultAcl(fullPath(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    super.removeAcl(fullPath(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    super.setAcl(fullPath(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return super.getAclStatus(fullPath(path));
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    super.setXAttr(fullPath(path), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return super.getXAttr(fullPath(path), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return super.getXAttrs(fullPath(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return super.getXAttrs(fullPath(path), names);
  }

  @Override
  public boolean truncate(Path path, long newLength) throws IOException {
    return super.truncate(fullPath(path), newLength);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return super.listXAttrs(fullPath(path));
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    super.removeXAttr(fullPath(path), name);
  }

  @Override
  public Path createSnapshot(Path path, String name) throws IOException {
    return super.createSnapshot(fullPath(path), name);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    super.renameSnapshot(fullPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    super.deleteSnapshot(fullPath(snapshotDir), snapshotName);
  }

  @Override
  public Path resolvePath(final Path p) throws IOException {
    return super.resolvePath(fullPath(p));
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return fs.getContentSummary(fullPath(f));
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    return fs.getQuotaUsage(fullPath(f));
  }

  private static Path rootPath = new Path(Path.SEPARATOR);

  @Override
  public long getDefaultBlockSize() {
    return getDefaultBlockSize(fullPath(rootPath));
  }
  
  @Override
  public long getDefaultBlockSize(Path f) {
    return super.getDefaultBlockSize(fullPath(f));
  }  

  @Override
  public short getDefaultReplication() {
    return getDefaultReplication(fullPath(rootPath));
  }

  @Override
  public short getDefaultReplication(Path f) {
    return super.getDefaultReplication(fullPath(f));
  }
  
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return getServerDefaults(fullPath(rootPath));
  }  

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    return super.getServerDefaults(fullPath(f));
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    return super.getStoragePolicy(fullPath(src));
  }

  @Override
  public void satisfyStoragePolicy(Path src) throws IOException {
    super.satisfyStoragePolicy(fullPath(src));
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    super.setStoragePolicy(fullPath(src), policyName);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    super.unsetStoragePolicy(fullPath(src));
  }

  @Override
  public FSDataOutputStreamBuilder createFile(final Path path) {
    return super.createFile(fullPath(path));
  }

  @Override
  public FutureDataInputStreamBuilder openFile(final Path path)
      throws IOException, UnsupportedOperationException {
    return super.openFile(fullPath(path));
  }
}
