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
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChrootedFs</code> is a file system with its root some path
 * below the root of its base file system.
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

 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFs extends AbstractFileSystem {
  private final AbstractFileSystem myFs;  // the base file system whose root is changed
  private final URI myUri; // the base URI + the chroot
  private final Path chRootPathPart; // the root below the root of the base
  private final String chRootPathPartString;
  
  protected AbstractFileSystem getMyFs() {
    return myFs;
  }
  
  /**
   * 
   * @param path
   * @return return full path including the chroot
   */
  protected Path fullPath(final Path path) {
    super.checkPath(path);
    return new Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
        + path.toUri().getPath());
  }

  @Override
  public boolean isValidName(String src) {
    return myFs.isValidName(fullPath(new Path(src)).toUri().toString());
  }

  public ChRootedFs(final AbstractFileSystem fs, final Path theRoot)
    throws URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(), false, fs.getUriDefaultPort());
    myFs = fs;
    myFs.checkPath(theRoot);
    chRootPathPart = new Path(myFs.getUriPath(theRoot));
    chRootPathPartString = chRootPathPart.toUri().getPath();
    /*
     * We are making URI include the chrootedPath: e.g. file:///chrootedPath.
     * This is questionable since Path#makeQualified(uri, path) ignores
     * the pathPart of a uri. Since this class is internal we can ignore
     * this issue but if we were to make it external then this needs
     * to be resolved.
     */
    // Handle the two cases:
    //              scheme:/// and scheme://authority/
    myUri = new URI(myFs.getUri().toString() + 
        (myFs.getUri().getAuthority() == null ? "" :  Path.SEPARATOR) +
          chRootPathPart.toUri().getPath().substring(1));
    super.checkPath(theRoot);
  }
  
  @Override
  public URI getUri() {
    return myUri;
  }

  
  /**
   *  
   * Strip out the root from the path.
   * 
   * @param p - fully qualified path p
   * @return -  the remaining path  without the beginning /
   */
  public String stripOutRoot(final Path p) {
    try {
     checkPath(p);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Internal Error - path " + p +
          " should have been with URI" + myUri);
    }
    String pathPart = p.toUri().getPath();
    return  (pathPart.length() == chRootPathPartString.length()) ?
        "" : pathPart.substring(chRootPathPartString.length() +
            (chRootPathPart.isRoot() ? 0 : 1));
  }
  

  @Override
  public Path getHomeDirectory() {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public Path getInitialWorkingDirectory() {
    /*
     * 3 choices here: return null or / or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     */
    return null;
  }
  
  
  public Path getResolvedQualifiedPath(final Path f)
      throws FileNotFoundException {
    return myFs.makeQualified(
        new Path(chRootPathPartString + f.toUri().toString()));
  }
  
  @Override
  public FSDataOutputStream createInternal(final Path f,
      final EnumSet<CreateFlag> flag, final FsPermission absolutePermission,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final ChecksumOpt checksumOpt,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    return myFs.createInternal(fullPath(f), flag,
        absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) 
      throws IOException, UnresolvedLinkException {
    return myFs.delete(fullPath(f), recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path f, final long start,
      final long len) throws IOException, UnresolvedLinkException {
    return myFs.getFileBlockLocations(fullPath(f), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileChecksum(fullPath(f));
  }

  @Override
  public FileStatus getFileStatus(final Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileStatus(fullPath(f));
  }

  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    myFs.access(fullPath(path), mode);
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) 
    throws IOException, UnresolvedLinkException {
    return myFs.getFileLinkStatus(fullPath(f));
  }
  
  @Override
  public FsStatus getFsStatus() throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(final Path f) throws IOException {
    return myFs.getServerDefaults(fullPath(f));
  }

  @Override
  public int getUriDefaultPort() {
    return myFs.getUriDefaultPort();
  }

  @Override
  public FileStatus[] listStatus(final Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.listStatus(fullPath(f));
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path f)
    throws IOException, UnresolvedLinkException {
    return myFs.listStatusIterator(fullPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws IOException, UnresolvedLinkException {
    return myFs.listLocatedStatus(fullPath(f));
  }

  @Override
  public void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    myFs.mkdir(fullPath(dir), permission, createParent);
    
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return myFs.open(fullPath(f), bufferSize);
  }

  @Override
  public boolean truncate(final Path f, final long newLength)
      throws IOException, UnresolvedLinkException {
    return myFs.truncate(fullPath(f), newLength);
  }

  @Override
  public void renameInternal(final Path src, final Path dst)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst));
  }
  
  @Override
  public void renameInternal(final Path src, final Path dst, 
      final boolean overwrite)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst), overwrite);
  }

  @Override
  public void setOwner(final Path f, final String username,
      final String groupname)
    throws IOException, UnresolvedLinkException {
    myFs.setOwner(fullPath(f), username, groupname);
    
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
    throws IOException, UnresolvedLinkException {
    myFs.setPermission(fullPath(f), permission);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
    throws IOException, UnresolvedLinkException {
    return myFs.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime) 
      throws IOException, UnresolvedLinkException {
    myFs.setTimes(fullPath(f), mtime, atime);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    myFs.modifyAclEntries(fullPath(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    myFs.removeAclEntries(fullPath(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    myFs.removeDefaultAcl(fullPath(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    myFs.removeAcl(fullPath(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    myFs.setAcl(fullPath(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return myFs.getAclStatus(fullPath(path));
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
                       EnumSet<XAttrSetFlag> flag) throws IOException {
    myFs.setXAttr(fullPath(path), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return myFs.getXAttr(fullPath(path), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return myFs.getXAttrs(fullPath(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return myFs.getXAttrs(fullPath(path), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return myFs.listXAttrs(fullPath(path));
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    myFs.removeXAttr(fullPath(path), name);
  }

  @Override
  public Path createSnapshot(Path path, String name) throws IOException {
    return myFs.createSnapshot(fullPath(path), name);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    myFs.renameSnapshot(fullPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    myFs.deleteSnapshot(fullPath(snapshotDir), snapshotName);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName)
    throws IOException {
    myFs.setStoragePolicy(fullPath(path), policyName);
  }

  @Override
  public void unsetStoragePolicy(final Path src)
    throws IOException {
    myFs.unsetStoragePolicy(fullPath(src));
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return myFs.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return myFs.getAllStoragePolicies();
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean supportsSymlinks() {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    /*
     * We leave the link alone:
     * If qualified or link relative then of course it is okay.
     * If absolute (ie / relative) then the link has to be resolved
     * relative to the changed root.
     */
    myFs.createSymlink(fullPath(target), link, createParent);
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    return myFs.getLinkTarget(fullPath(f));
  }
  
  
  @Override
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }
}
