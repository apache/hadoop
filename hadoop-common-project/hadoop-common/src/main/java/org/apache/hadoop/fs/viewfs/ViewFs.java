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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;


/**
 * ViewFs (extends the AbstractFileSystem interface) implements a client-side
 * mount table. The viewFs file system is implemented completely in memory on
 * the client side. The client-side mount table allows a client to provide a 
 * customized view of a file system namespace that is composed from 
 * one or more individual file systems (a localFs or Hdfs, S3fs, etc).
 * For example one could have a mount table that provides links such as
 * <ul>
 * <li>  /user          -> hdfs://nnContainingUserDir/user
 * <li>  /project/foo   -> hdfs://nnProject1/projects/foo
 * <li>  /project/bar   -> hdfs://nnProject2/projects/bar
 * <li>  /tmp           -> hdfs://nnTmp/privateTmpForUserXXX
 * </ul> 
 * 
 * ViewFs is specified with the following URI: <b>viewfs:///</b> 
 * <p>
 * To use viewfs one would typically set the default file system in the
 * config  (i.e. fs.default.name< = viewfs:///) along with the
 * mount table config variables as described below. 
 * 
 * <p>
 * <b> ** Config variables to specify the mount table entries ** </b>
 * <p>
 * 
 * The file system is initialized from the standard Hadoop config through
 * config variables.
 * See {@link FsConstants} for URI and Scheme constants; 
 * See {@link Constants} for config var constants; 
 * see {@link ConfigUtil} for convenient lib.
 * 
 * <p>
 * All the mount table config entries for view fs are prefixed by 
 * <b>fs.viewfs.mounttable.</b>
 * For example the above example can be specified with the following
 *  config variables:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.link./user=
 *  hdfs://nnContainingUserDir/user
 *  <li> fs.viewfs.mounttable.default.link./project/foo=
 *  hdfs://nnProject1/projects/foo
 *  <li> fs.viewfs.mounttable.default.link./project/bar=
 *  hdfs://nnProject2/projects/bar
 *  <li> fs.viewfs.mounttable.default.link./tmp=
 *  hdfs://nnTmp/privateTmpForUserXXX
 *  </ul>
 *  
 * The default mount table (when no authority is specified) is 
 * from config variables prefixed by <b>fs.viewFs.mounttable.default </b>
 * The authority component of a URI can be used to specify a different mount
 * table. For example,
 * <ul>
 * <li>  viewfs://sanjayMountable/
 * </ul>
 * is initialized from fs.viewFs.mounttable.sanjayMountable.* config variables.
 * 
 *  <p> 
 *  <b> **** Merge Mounts **** </b>(NOTE: merge mounts are not implemented yet.)
 *  <p>
 *  
 *   One can also use "MergeMounts" to merge several directories (this is
 *   sometimes  called union-mounts or junction-mounts in the literature.
 *   For example of the home directories are stored on say two file systems
 *   (because they do not fit on one) then one could specify a mount
 *   entry such as following merges two dirs:
 *   <ul>
 *   <li> /user -> hdfs://nnUser1/user,hdfs://nnUser2/user
 *   </ul>
 *  Such a mergeLink can be specified with the following config var where ","
 *  is used as the separator for each of links to be merged:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.linkMerge./user=
 *  hdfs://nnUser1/user,hdfs://nnUser1/user
 *  </ul>
 *   A special case of the merge mount is where mount table's root is merged
 *   with the root (slash) of another file system:
 *   <ul>
 *   <li>    fs.viewfs.mounttable.default.linkMergeSlash=hdfs://nn99/
 *   </ul>
 *   In this cases the root of the mount table is merged with the root of
 *            <b>hdfs://nn99/ </b> 
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFs extends AbstractFileSystem {
  final long creationTime; // of the the mount table
  final UserGroupInformation ugi; // the user/group of user who created mtable
  final Configuration config;
  InodeTree<AbstractFileSystem> fsState;  // the fs state; ie the mount table
  Path homeDir = null;
  
  static AccessControlException readOnlyMountTable(final String operation,
      final String p) {
    return new AccessControlException( 
        "InternalDir of ViewFileSystem is readonly; operation=" + operation + 
        "Path=" + p);
  }
  static AccessControlException readOnlyMountTable(final String operation,
      final Path p) {
    return readOnlyMountTable(operation, p.toString());
  }
  
  
  static public class MountPoint {
    private Path src;       // the src of the mount
    private URI[] targets; //  target of the mount; Multiple targets imply mergeMount
    MountPoint(Path srcPath, URI[] targetURIs) {
      src = srcPath;
      targets = targetURIs;
    }
    Path getSrc() {
      return src;
    }
    URI[] getTargets() {
      return targets;
    }
  }
  
  public ViewFs(final Configuration conf) throws IOException,
      URISyntaxException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   * 
   * @param theUri which must be that of ViewFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException 
   */
  ViewFs(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, FsConstants.VIEWFS_SCHEME, false, -1);
    creationTime = Time.now();
    ugi = UserGroupInformation.getCurrentUser();
    config = conf;
    // Now build  client side view (i.e. client side mount table) from config.
    String authority = theUri.getAuthority();
    fsState = new InodeTree<AbstractFileSystem>(conf, authority) {

      @Override
      protected
      AbstractFileSystem getTargetFileSystem(final URI uri)
        throws URISyntaxException, UnsupportedFileSystemException {
          String pathString = uri.getPath();
          if (pathString.isEmpty()) {
            pathString = "/";
          }
          return new ChRootedFs(
              AbstractFileSystem.createFileSystem(uri, config),
              new Path(pathString));
      }

      @Override
      protected
      AbstractFileSystem getTargetFileSystem(
          final INodeDir<AbstractFileSystem> dir) throws URISyntaxException {
        return new InternalDirOfViewFs(dir, creationTime, ugi, getUri());
      }

      @Override
      protected
      AbstractFileSystem getTargetFileSystem(URI[] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        throw new UnsupportedFileSystemException("mergefs not implemented yet");
        // return MergeFs.createMergeFs(mergeFsURIList, config);
      }
    };
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    return LocalConfigKeys.getServerDefaults(); 
  }

  @Override
  public FsServerDefaults getServerDefaults(final Path f) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), true);
    } catch (FileNotFoundException fnfe) {
      return LocalConfigKeys.getServerDefaults();
    }
    return res.targetFileSystem.getServerDefaults(res.remainingPath);
  }

  @Override
  public int getUriDefaultPort() {
    return -1;
  }
 
  @Override
  public Path getHomeDirectory() {
    if (homeDir == null) {
      String base = fsState.getHomeDirPrefixValue();
      if (base == null) {
        base = "/user";
      }
      homeDir = (base.equals("/") ? 
        this.makeQualified(new Path(base + ugi.getShortUserName())):
        this.makeQualified(new Path(base + "/" + ugi.getShortUserName())));
    }
    return homeDir;
  }
  
  @Override
  public Path resolvePath(final Path f) throws FileNotFoundException,
          AccessControlException, UnresolvedLinkException, IOException {
    final InodeTree.ResolveResult<AbstractFileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);

  }
  
  @Override
  public FSDataOutputStream createInternal(final Path f,
      final EnumSet<CreateFlag> flag, final FsPermission absolutePermission,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress, final ChecksumOpt checksumOpt,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("create", f);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createInternal(res.remainingPath, flag,
        absolutePermission, bufferSize, replication,
        blockSize, progress, checksumOpt,
        createParent);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw new AccessControlException(
          "Cannot delete internal mount table directory: " + f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path f, final long start,
      final long len) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return
      res.targetFileSystem.getFileBlockLocations(res.remainingPath, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);

    //  FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFsFileStatus that works around.
    
    
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new ViewFsFileStatus(status, this.makeQualified(f));
  }

  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
      fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.access(res.remainingPath, mode);
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f)
     throws AccessControlException, FileNotFoundException,
     UnsupportedFileSystemException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getFileLinkStatus(res.remainingPath);
  }
  
  @Override
  public FsStatus getFsStatus() throws AccessControlException,
      FileNotFoundException, IOException {
    return new FsStatus(0, 0, 0);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    final InodeTree.ResolveResult<AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    final RemoteIterator<FileStatus> fsIter =
      res.targetFileSystem.listStatusIterator(res.remainingPath);
    if (res.isInternalDir()) {
      return fsIter;
    }

    return new WrappingRemoteIterator<FileStatus>(res, fsIter, f) {
      @Override
      public FileStatus getViewFsFileStatus(FileStatus stat, Path newPath) {
        return new ViewFsFileStatus(stat, newPath);
      }
    };
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    final InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(f), true);
    final RemoteIterator<LocatedFileStatus> fsIter =
        res.targetFileSystem.listLocatedStatus(res.remainingPath);
    if (res.isInternalDir()) {
      return fsIter;
    }

    return new WrappingRemoteIterator<LocatedFileStatus>(res, fsIter, f) {
      @Override
      public LocatedFileStatus getViewFsFileStatus(LocatedFileStatus stat,
          Path newPath) {
        return new ViewFsLocatedFileStatus(stat, newPath);
      }
    };
  }
  
  @Override
  public FileStatus[] listStatus(final Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    FileStatus[] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      ChRootedFs targetFs;
      targetFs = (ChRootedFs) res.targetFileSystem;
      int i = 0;
      for (FileStatus status : statusLst) {
          String suffix = targetFs.stripOutRoot(status.getPath());
          statusLst[i++] = new ViewFsFileStatus(status, this.makeQualified(
              suffix.length() == 0 ? f : new Path(res.resolvedPath, suffix)));
      }
    }
    return statusLst;
  }

  @Override
  public void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(dir), false);
    res.targetFileSystem.mkdir(res.remainingPath, permission, createParent);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  @Override
  public boolean truncate(final Path f, final long newLength)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.truncate(res.remainingPath, newLength);
  }

  @Override
  public void renameInternal(final Path src, final Path dst,
      final boolean overwrite) throws IOException, UnresolvedLinkException {
    // passing resolveLastComponet as false to catch renaming a mount point 
    // itself we need to catch this as an internal operation and fail.
    InodeTree.ResolveResult<AbstractFileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw new AccessControlException(
          "Cannot Rename within internal dirs of mount table: src=" + src
              + " is readOnly");
    }

    InodeTree.ResolveResult<AbstractFileSystem> resDst = 
                                fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
      throw new AccessControlException(
          "Cannot Rename within internal dirs of mount table: dest=" + dst
              + " is readOnly");
    }
    
    /**
    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets. 
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.targetFileSystem.getUri().equals(
              resDst.targetFileSystem.getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    */
    
    //
    // Alternate 3 : renames ONLY within the the same mount links.
    //

    if (resSrc.targetFileSystem !=resDst.targetFileSystem) {
      throw new IOException("Renames across Mount points not supported");
    }
    
    resSrc.targetFileSystem.renameInternal(resSrc.remainingPath,
      resDst.remainingPath, overwrite);
  }

  @Override
  public void renameInternal(final Path src, final Path dst)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, IOException {
    renameInternal(src, dst, false);
  }
  
  @Override
  public boolean supportsSymlinks() {
    return true;
  }
  
  @Override
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    InodeTree.ResolveResult<AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(link), false);
    } catch (FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("createSymlink", link);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    res.targetFileSystem.createSymlink(target, res.remainingPath,
        createParent);  
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getLinkTarget(res.remainingPath);
  }

  @Override
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
    
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum)
      throws AccessControlException, IOException {
    // This is a file system level operations, however ViewFs 
    // points to many file systems. Noop for ViewFs. 
  }
  
  public MountPoint[] getMountPoints() {
    List<InodeTree.MountPoint<AbstractFileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    MountPoint[] result = new MountPoint[mountPoints.size()];
    for ( int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new MountPoint(new Path(mountPoints.get(i).src), 
                              mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }
  
  @Override
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    List<InodeTree.MountPoint<AbstractFileSystem>> mountPoints = 
                fsState.getMountPoints();
    int initialListSize  = 0;
    for (InodeTree.MountPoint<AbstractFileSystem> im : mountPoints) {
      initialListSize += im.target.targetDirLinkList.length; 
    }
    List<Token<?>> result = new ArrayList<Token<?>>(initialListSize);
    for ( int i = 0; i < mountPoints.size(); ++i ) {
      List<Token<?>> tokens = 
        mountPoints.get(i).target.targetFileSystem.getDelegationTokens(renewer);
      if (tokens != null) {
        result.addAll(tokens);
      }
    }
    return result;
  }

  @Override
  public boolean isValidName(String src) {
    // Prefix validated at mount time and rest of path validated by mount target.
    return true;
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.modifyAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeDefaultAcl(res.remainingPath);
  }

  @Override
  public void removeAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeAcl(res.remainingPath);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setAcl(res.remainingPath, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getAclStatus(res.remainingPath);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
                       EnumSet<XAttrSetFlag> flag) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setXAttr(res.remainingPath, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttr(res.remainingPath, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.listXAttrs(res.remainingPath);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeXAttr(res.remainingPath, name);
  }

  /**
   * Helper class to perform some transformation on results returned
   * from a RemoteIterator.
   */
  private abstract class WrappingRemoteIterator<T extends FileStatus>
      implements RemoteIterator<T> {
    private final String resolvedPath;
    private final ChRootedFs targetFs;
    private final RemoteIterator<T> innerIter;
    private final Path originalPath;

    WrappingRemoteIterator(InodeTree.ResolveResult<AbstractFileSystem> res,
        RemoteIterator<T> innerIter, Path originalPath) {
      this.resolvedPath = res.resolvedPath;
      this.targetFs = (ChRootedFs)res.targetFileSystem;
      this.innerIter = innerIter;
      this.originalPath = originalPath;
    }

    @Override
    public boolean hasNext() throws IOException {
      return innerIter.hasNext();
    }

    @Override
    public T next() throws IOException {
      T status =  innerIter.next();
      String suffix = targetFs.stripOutRoot(status.getPath());
      Path newPath = makeQualified(suffix.length() == 0 ? originalPath
          : new Path(resolvedPath, suffix));
      return getViewFsFileStatus(status, newPath);
    }

    protected abstract T getViewFsFileStatus(T status, Path newPath);
  }

  /*
   * An instance of this class represents an internal dir of the viewFs 
   * ie internal dir of the mount table.
   * It is a ready only mount tbale and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends AbstractFileSystem {
    
    final InodeTree.INodeDir<AbstractFileSystem>  theInternalDir;
    final long creationTime; // of the the mount table
    final UserGroupInformation ugi; // the user/group of user who created mtable
    final URI myUri; // the URI of the outer ViewFs
    
    public InternalDirOfViewFs(final InodeTree.INodeDir<AbstractFileSystem> dir,
        final long cTime, final UserGroupInformation ugi, final URI uri)
      throws URISyntaxException {
      super(FsConstants.VIEWFS_URI, FsConstants.VIEWFS_SCHEME, false, -1);
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
      myUri = uri;
    }

    static private void checkPathIsSlash(final Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }

    @Override
    public FSDataOutputStream createInternal(final Path f,
        final EnumSet<CreateFlag> flag, final FsPermission absolutePermission,
        final int bufferSize, final short replication, final long blockSize,
        final Progressable progress, final ChecksumOpt checksumOpt,
        final boolean createParent) throws AccessControlException,
        FileAlreadyExistsException, FileNotFoundException,
        ParentNotDirectoryException, UnsupportedFileSystemException,
        UnresolvedLinkException, IOException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public boolean delete(final Path f, final boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(final Path f, final long start,
        final long len) throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileChecksum getFileChecksum(final Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileStatus getFileStatus(final Path f) throws IOException {
      checkPathIsSlash(f);
      return new FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_555, ugi.getUserName(), ugi.getGroupNames()[0],
          new Path(theInternalDir.fullPath).makeQualified(
              myUri, null));
    }
    
    @Override
    public FileStatus getFileLinkStatus(final Path f)
        throws FileNotFoundException {
      // look up i internalDirs children - ignore first Slash
      INode<AbstractFileSystem> inode =
        theInternalDir.children.get(f.toUri().toString().substring(1)); 
      if (inode == null) {
        throw new FileNotFoundException(
            "viewFs internal mount table - missing entry:" + f);
      }
      FileStatus result;
      if (inode instanceof INodeLink) {
        INodeLink<AbstractFileSystem> inodelink = 
          (INodeLink<AbstractFileSystem>) inode;
        result = new FileStatus(0, false, 0, 0, creationTime, creationTime,
            PERMISSION_555, ugi.getUserName(), ugi.getGroupNames()[0],
            inodelink.getTargetLink(),
            new Path(inode.fullPath).makeQualified(
                myUri, null));
      } else {
        result = new FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_555, ugi.getUserName(), ugi.getGroupNames()[0],
          new Path(inode.fullPath).makeQualified(
              myUri, null));
      }
      return result;
    }
    
    @Override
    public FsStatus getFsStatus() {
      return new FsStatus(0, 0, 0);
    }

    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
      return LocalConfigKeys.getServerDefaults();
    }

    @Override
    public FsServerDefaults getServerDefaults(final Path f) throws IOException {
      return LocalConfigKeys.getServerDefaults();
    }

    @Override
    public int getUriDefaultPort() {
      return -1;
    }

    @Override
    public FileStatus[] listStatus(final Path f) throws AccessControlException,
        IOException {
      checkPathIsSlash(f);
      FileStatus[] result = new FileStatus[theInternalDir.children.size()];
      int i = 0;
      for (Entry<String, INode<AbstractFileSystem>> iEntry : 
                                          theInternalDir.children.entrySet()) {
        INode<AbstractFileSystem> inode = iEntry.getValue();

        
        if (inode instanceof INodeLink ) {
          INodeLink<AbstractFileSystem> link = 
            (INodeLink<AbstractFileSystem>) inode;

          result[i++] = new FileStatus(0, false, 0, 0,
            creationTime, creationTime,
            PERMISSION_555, ugi.getUserName(), ugi.getGroupNames()[0],
            link.getTargetLink(),
            new Path(inode.fullPath).makeQualified(
                myUri, null));
        } else {
          result[i++] = new FileStatus(0, true, 0, 0,
            creationTime, creationTime,
            PERMISSION_555, ugi.getUserName(), ugi.getGroupNames()[0],
            new Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      }
      return result;
    }

    @Override
    public void mkdir(final Path dir, final FsPermission permission,
        final boolean createParent) throws AccessControlException,
        FileAlreadyExistsException {
      if (theInternalDir.isRoot && dir == null) {
        throw new FileAlreadyExistsException("/ already exits");
      }
      throw readOnlyMountTable("mkdir", dir);
    }

    @Override
    public FSDataInputStream open(final Path f, final int bufferSize)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public boolean truncate(final Path f, final long newLength)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("truncate", f);
    }

    @Override
    public void renameInternal(final Path src, final Path dst)
        throws AccessControlException, IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public boolean supportsSymlinks() {
      return true;
    }
    
    @Override
    public void createSymlink(final Path target, final Path link,
        final boolean createParent) throws AccessControlException {
      throw readOnlyMountTable("createSymlink", link);    
    }

    @Override
    public Path getLinkTarget(final Path f) throws FileNotFoundException,
        IOException {
      return getFileLinkStatus(f).getSymlink();
    }

    @Override
    public void setOwner(final Path f, final String username,
        final String groupname) throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(final Path f, final FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public boolean setReplication(final Path f, final short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(final Path f, final long mtime, final long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(final boolean verifyChecksum)
        throws AccessControlException {
      throw readOnlyMountTable("setVerifyChecksum", "");   
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("modifyAclEntries", path);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAclEntries", path);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeDefaultAcl", path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAcl", path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setAcl", path);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
      checkPathIsSlash(path);
      return new AclStatus.Builder().owner(ugi.getUserName())
          .group(ugi.getGroupNames()[0])
          .addEntries(AclUtil.getMinimalAcl(PERMISSION_555))
          .stickyBit(false).build();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
                         EnumSet<XAttrSetFlag> flag) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setXAttr", path);
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
      throw readOnlyMountTable("removeXAttr", path);
    }
  }
}
