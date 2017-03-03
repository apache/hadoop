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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * A <code>FilterFileSystem</code> contains
 * some other file system, which it uses as
 * its  basic file system, possibly transforming
 * the data along the way or providing  additional
 * functionality. The class <code>FilterFileSystem</code>
 * itself simply overrides all  methods of
 * <code>FileSystem</code> with versions that
 * pass all requests to the contained  file
 * system. Subclasses of <code>FilterFileSystem</code>
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterFileSystem extends FileSystem {
  
  protected FileSystem fs;
  protected String swapScheme;
  
  /*
   * so that extending classes can define it
   */
  public FilterFileSystem() {
  }
  
  public FilterFileSystem(FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }

  /**
   * Get the raw file system 
   * @return FileSystem being filtered
   */
  public FileSystem getRawFileSystem() {
    return fs;
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    // this is less than ideal, but existing filesystems sometimes neglect
    // to initialize the embedded filesystem
    if (fs.getConf() == null) {
      fs.initialize(name, conf);
    }
    String scheme = name.getScheme();
    if (!scheme.equals(fs.getUri().getScheme())) {
      swapScheme = scheme;
    }
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  @Override
  public URI getUri() {
    return fs.getUri();
  }
  
  
  @Override
  protected URI getCanonicalUri() {
    return fs.getCanonicalUri();
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return fs.canonicalizeUri(uri);
  }

  /** Make sure that a path specifies a FileSystem. */
  @Override
  public Path makeQualified(Path path) {
    Path fqPath = fs.makeQualified(path);
    // swap in our scheme if the filtered fs is using a different scheme
    if (swapScheme != null) {
      try {
        // NOTE: should deal with authority, but too much other stuff is broken 
        fqPath = new Path(
            new URI(swapScheme, fqPath.toUri().getSchemeSpecificPart(), null)
        );
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return fqPath;
  }
  
  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  /** Check that a Path belongs to this FileSystem. */
  @Override
  protected void checkPath(Path path) {
    fs.checkPath(path);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
    long len) throws IOException {
      return fs.getFileBlockLocations(file, start, len);
  }

  @Override
  public Path resolvePath(final Path p) throws IOException {
    return fs.resolvePath(p);
  }
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return fs.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return fs.append(f, bufferSize, progress);
  }

  @Override
  public void concat(Path f, Path[] psrcs) throws IOException {
    fs.concat(f, psrcs);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return fs.create(f, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f,
        FsPermission permission,
        EnumSet<CreateFlag> flags,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress,
        ChecksumOpt checksumOpt) throws IOException {
    return fs.create(f, permission,
      flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
  throws FileNotFoundException, IOException {
    return fs.listLocatedStatus(f, filter);
  }


  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    
    return fs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  /**
   * Set replication for an existing file.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    return fs.setReplication(src, replication);
  }
  
  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return fs.rename(src, dst);
  }

  @Override
  protected void rename(Path src, Path dst, Rename... options)
      throws IOException {
    fs.rename(src, dst, options);
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    return fs.truncate(f, newLength);
  }
  
  /** Delete a file */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }
  
  /** List files in a directory. */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    return fs.listCorruptFileBlocks(path);
  }

  /** List files and its block locations in a directory. */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
  throws IOException {
    return fs.listLocatedStatus(f);
  }
  
  /** Return a remote iterator for listing in a directory */
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f)
  throws IOException {
    return fs.listStatusIterator(f);
   }

  @Override
  public Path getHomeDirectory() {
    return fs.getHomeDirectory();
  }


  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   * 
   * @param newDir
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    fs.setWorkingDirectory(newDir);
  }
  
  /**
   * Get the current working directory for the given file system
   * 
   * @return the directory pathname
   */
  @Override
  public Path getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    return fs.getInitialWorkingDirectory();
  }
  
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return fs.getStatus(p);
  }
  
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return fs.mkdirs(f, permission);
  }


  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, src, dst);
  }
  
  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path[] srcs, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   */   
  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    fs.copyToLocalFile(delSrc, src, dst);
  }
  
  /**
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   */
  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   */
  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /** Return the total size of all files in the filesystem.*/
  @Override
  public long getUsed() throws IOException{
    return fs.getUsed();
  }

  /** Return the total size of all files from a specified path.*/
  @Override
  public long getUsed(Path path) throws IOException {
    return fs.getUsed(path);
  }

  @Override
  public long getDefaultBlockSize() {
    return fs.getDefaultBlockSize();
  }
  
  @Override
  public short getDefaultReplication() {
    return fs.getDefaultReplication();
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fs.getServerDefaults();
  }

  // path variants delegate to underlying filesystem 
  @Override
  public long getDefaultBlockSize(Path f) {
    return fs.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication(Path f) {
    return fs.getDefaultReplication(f);
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    return fs.getServerDefaults(f);
  }

  /**
   * Get file status.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return fs.getFileStatus(f);
  }

  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, IOException {
    fs.access(path, mode);
  }

  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    fs.createSymlink(target, link, createParent);
  }

  public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fs.getFileLinkStatus(f);
  }

  public boolean supportsSymlinks() {
    return fs.supportsSymlinks();
  }

  public Path getLinkTarget(Path f) throws IOException {
    return fs.getLinkTarget(f);
  }

  protected Path resolveLink(Path f) throws IOException {
    return fs.resolveLink(f);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return fs.getFileChecksum(f);
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    return fs.getFileChecksum(f, length);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }
  
  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    fs.setWriteChecksum(writeChecksum);
  }

  @Override
  public Configuration getConf() {
    return fs.getConf();
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    fs.close();
  }

  @Override
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    fs.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    fs.setTimes(p, mtime, atime);
  }

  @Override
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    fs.setPermission(p, permission);
  }

  @Override
  protected FSDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag,
      int bufferSize, short replication, long blockSize,
      Progressable progress, ChecksumOpt checksumOpt)
      throws IOException {
    return fs.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected boolean primitiveMkdir(Path f, FsPermission abdolutePermission)
      throws IOException {
    return fs.primitiveMkdir(f, abdolutePermission);
  }
  
  @Override // FileSystem
  public FileSystem[] getChildFileSystems() {
    return new FileSystem[]{fs};
  }

  @Override // FileSystem
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    return fs.createSnapshot(path, snapshotName);
  }
  
  @Override // FileSystem
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }
  
  @Override // FileSystem
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    fs.deleteSnapshot(path, snapshotName);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    fs.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    fs.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    fs.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    fs.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    fs.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return fs.getAclStatus(path);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value)
      throws IOException {
    fs.setXAttr(path, name, value);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    fs.setXAttr(path, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return fs.getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return fs.getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    return fs.getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return fs.listXAttrs(path);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    fs.removeXAttr(path, name);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName)
      throws IOException {
    fs.setStoragePolicy(src, policyName);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    fs.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return fs.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return fs.getAllStoragePolicies();
  }

  @Override
  public Path getTrashRoot(Path path) {
    return fs.getTrashRoot(path);
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    return fs.getTrashRoots(allUsers);
  }
}
