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
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * A <code>FilterFs</code> contains some other file system, which it uses as its
 * basic file system, possibly transforming the data along the way or providing
 * additional functionality. The class <code>FilterFs</code> itself simply
 * overrides all methods of <code>AbstractFileSystem</code> with versions that
 * pass all requests to the contained file system. Subclasses of
 * <code>FilterFs</code> may further override some of these methods and may also
 * provide additional methods and fields.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class FilterFs extends AbstractFileSystem {
  private final AbstractFileSystem myFs;
  
  protected AbstractFileSystem getMyFs() {
    return myFs;
  }
  
  protected FilterFs(AbstractFileSystem fs) throws URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(), false, fs.getUriDefaultPort());
    myFs = fs;
  }

  @Override
  public Statistics getStatistics() {
    return myFs.getStatistics();
  }
  
  @Override
  public Path makeQualified(Path path) {
    return myFs.makeQualified(path);
  }

  @Override
  public Path getInitialWorkingDirectory() {
    return myFs.getInitialWorkingDirectory();
  }
  
  @Override
  public Path getHomeDirectory() {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public FSDataOutputStream createInternal(Path f,
    EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize,
    short replication, long blockSize, Progressable progress,
    ChecksumOpt checksumOpt, boolean createParent) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.createInternal(f, flag, absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public boolean delete(Path f, boolean recursive) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.delete(f, recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileBlockLocations(f, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileChecksum(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileStatus(f);
  }

  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    checkPath(path);
    myFs.access(path, mode);
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileLinkStatus(f);
  }
  
  @Override
  public FsStatus getFsStatus(final Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    return myFs.getFsStatus(f);
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
    return myFs.getServerDefaults(f);
  }

  @Override
  public Path resolvePath(final Path p) throws FileNotFoundException,
        UnresolvedLinkException, AccessControlException, IOException {
    return myFs.resolvePath(p);
  }

  @Override
  public int getUriDefaultPort() {
    return myFs.getUriDefaultPort();
  }

  @Override
  public URI getUri() {
    return myFs.getUri();
  }
  
  @Override
  public void checkPath(Path path) {
    myFs.checkPath(path);
  }
  
  @Override
  public String getUriPath(final Path p) {
    return myFs.getUriPath(p);
  }
  
  @Override
  public FileStatus[] listStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.listStatus(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.listLocatedStatus(f);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    return myFs.listCorruptFileBlocks(path);
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException, UnresolvedLinkException {
    checkPath(dir);
    myFs.mkdir(dir, permission, createParent);
    
  }

  @Override
  public FSDataInputStream open(final Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.open(f);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.open(f, bufferSize);
  }

  @Override
  public boolean truncate(Path f, long newLength) 
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.truncate(f, newLength);
  }

  @Override
  public void renameInternal(Path src, Path dst) 
    throws IOException, UnresolvedLinkException {
    checkPath(src);
    checkPath(dst);
    myFs.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void renameInternal(final Path src, final Path dst,
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    myFs.renameInternal(src, dst, overwrite);
  }
  
  @Override
  public void setOwner(Path f, String username, String groupname)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setOwner(f, username, groupname);
    
  }

  @Override
  public void setPermission(Path f, FsPermission permission)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setPermission(f, permission);
  }

  @Override
  public boolean setReplication(Path f, short replication)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.setReplication(f, replication);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) 
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean supportsSymlinks() {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) 
    throws IOException, UnresolvedLinkException {
    myFs.createSymlink(target, link, createParent);
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    return myFs.getLinkTarget(f);
  }
  
  @Override // AbstractFileSystem
  public String getCanonicalServiceName() {
    return myFs.getCanonicalServiceName();
  }
  
  @Override // AbstractFileSystem
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }

  @Override
  public boolean isValidName(String src) {
    return myFs.isValidName(src);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    myFs.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    myFs.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    myFs.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    myFs.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    myFs.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return myFs.getAclStatus(path);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value)
      throws IOException {
    myFs.setXAttr(path, name, value);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    myFs.setXAttr(path, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return myFs.getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return myFs.getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return myFs.getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return myFs.listXAttrs(path);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    myFs.removeXAttr(path, name);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    return myFs.createSnapshot(path, snapshotName);
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    myFs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    myFs.deleteSnapshot(path, snapshotName);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName)
      throws IOException {
    myFs.setStoragePolicy(path, policyName);
  }

  @Override
  public void unsetStoragePolicy(final Path src)
      throws IOException {
    myFs.unsetStoragePolicy(src);
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
}
