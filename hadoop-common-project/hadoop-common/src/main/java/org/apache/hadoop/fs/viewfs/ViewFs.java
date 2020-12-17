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

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS_DEFAULT;
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ViewFs (extends the AbstractFileSystem interface) implements a client-side
 * mount table. The viewFs file system is implemented completely in memory on
 * the client side. The client-side mount table allows a client to provide a 
 * customized view of a file system namespace that is composed from 
 * one or more individual file systems (a localFs or Hdfs, S3fs, etc).
 * For example one could have a mount table that provides links such as
 * <ul>
 * <li>  /user          {@literal ->} hdfs://nnContainingUserDir/user
 * <li>  /project/foo   {@literal ->} hdfs://nnProject1/projects/foo
 * <li>  /project/bar   {@literal ->} hdfs://nnProject2/projects/bar
 * <li>  /tmp           {@literal ->} hdfs://nnTmp/privateTmpForUserXXX
 * </ul> 
 * 
 * ViewFs is specified with the following URI: <b>viewfs:///</b> 
 * <p>
 * To use viewfs one would typically set the default file system in the
 * config  (i.e. fs.defaultFS {@literal <} = viewfs:///) along with the
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
 *   <li> /user {@literal ->} hdfs://nnUser1/user,hdfs://nnUser2/user
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
  static final Logger LOG = LoggerFactory.getLogger(ViewFs.class);
  final long creationTime; // of the the mount table
  final UserGroupInformation ugi; // the user/group of user who created mtable
  final Configuration config;
  InodeTree<AbstractFileSystem> fsState;  // the fs state; ie the mount table
  Path homeDir = null;
  private ViewFileSystem.RenameStrategy renameStrategy =
      ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT;
  private static boolean showMountLinksAsSymlinks = true;

  static AccessControlException readOnlyMountTable(final String operation,
      final String p) {
    return new AccessControlException(
        "InternalDir of ViewFileSystem is readonly, operation " + operation +
            " not permitted on path " + p + ".");
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

  /**
   * Returns the ViewFileSystem type.
   *
   * @return <code>viewfs</code>
   */
  String getType() {
    return FsConstants.VIEWFS_TYPE;
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
    showMountLinksAsSymlinks = config
        .getBoolean(CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
            CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS_DEFAULT);
    // Now build  client side view (i.e. client side mount table) from config.
    String authority = theUri.getAuthority();
    boolean initingUriAsFallbackOnNoMounts =
        !FsConstants.VIEWFS_TYPE.equals(getType());
    fsState = new InodeTree<AbstractFileSystem>(conf, authority, theUri,
        initingUriAsFallbackOnNoMounts) {

      @Override
      protected AbstractFileSystem getTargetFileSystem(final URI uri)
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
      protected AbstractFileSystem getTargetFileSystem(
          final INodeDir<AbstractFileSystem> dir) throws URISyntaxException {
        return new InternalDirOfViewFs(dir, creationTime, ugi, getUri(), this,
            config);
      }

      @Override
      protected AbstractFileSystem getTargetFileSystem(final String settings,
          final URI[] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        throw new UnsupportedFileSystemException("mergefs not implemented yet");
        // return MergeFs.createMergeFs(mergeFsURIList, config);
      }
    };
    renameStrategy = ViewFileSystem.RenameStrategy.valueOf(
        conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
            ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString()));
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

  /**
   * {@inheritDoc}
   *
   * If the given path is a symlink(mount link), the path will be resolved to a
   * target path and it will get the resolved path's FileStatus object. It will
   * not be represented as a symlink and isDirectory API returns true if the
   * resolved path is a directory, false otherwise.
   */
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
  
  /**
   * {@inheritDoc}
   *
   * Note: listStatus considers listing from fallbackLink if available. If the
   * same directory path is present in configured mount path as well as in
   * fallback fs, then only the fallback path will be listed in the returned
   * result except for link.
   *
   * If any of the the immediate children of the given path f is a symlink(mount
   * link), the returned FileStatus object of that children would be represented
   * as a symlink. It will not be resolved to the target path and will not get
   * the target path FileStatus object. The target path will be available via
   * getSymlink on that children's FileStatus object. Since it represents as
   * symlink, isDirectory on that children's FileStatus will return false.
   * This behavior can be changed by setting an advanced configuration
   * fs.viewfs.mount.links.as.symlinks to false. In this case, mount points will
   * be represented as non-symlinks and all the file/directory attributes like
   * permissions, isDirectory etc will be assigned from it's resolved target
   * directory/file.
   *
   * If you want to get the FileStatus of target path for that children, you may
   * want to use GetFileStatus API with that children's symlink path. Please see
   * {@link ViewFs#getFileStatus(Path f)}
   *
   * Note: In ViewFs, by default the mount links are represented as symlinks.
   */
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
    // itself we need to catch this as an internal operation and fail if no
    // fallback.
    InodeTree.ResolveResult<AbstractFileSystem> resSrc =
        fsState.resolve(getUriPath(src), false);

    if (resSrc.isInternalDir()) {
      if (fsState.getRootFallbackLink() == null) {
        // If fallback is null, we can't rename from src.
        throw new AccessControlException(
            "Cannot Rename within internal dirs of mount table: src=" + src
                + " is readOnly");
      }
      InodeTree.ResolveResult<AbstractFileSystem> resSrcWithLastComp =
          fsState.resolve(getUriPath(src), true);
      if (resSrcWithLastComp.isInternalDir() || resSrcWithLastComp
          .isLastInternalDirLink()) {
        throw new AccessControlException(
            "Cannot Rename within internal dirs of mount table: src=" + src
                + " is readOnly");
      } else {
        // This is fallback and let's set the src fs with this fallback
        resSrc = resSrcWithLastComp;
      }
    }

    InodeTree.ResolveResult<AbstractFileSystem> resDst =
        fsState.resolve(getUriPath(dst), false);

    if (resDst.isInternalDir()) {
      if (fsState.getRootFallbackLink() == null) {
        // If fallback is null, we can't rename to dst.
        throw new AccessControlException(
            "Cannot Rename within internal dirs of mount table: dest=" + dst
                + " is readOnly");
      }
      // if the fallback exist, we may have chance to rename to fallback path
      // where dst parent is matching to internalDir.
      InodeTree.ResolveResult<AbstractFileSystem> resDstWithLastComp =
          fsState.resolve(getUriPath(dst), true);
      if (resDstWithLastComp.isInternalDir()) {
        // We need to get fallback here. If matching fallback path not exist, it
        // will fail later. This is a very special case: Even though we are on
        // internal directory, we should allow to rename, so that src files will
        // moved under matching fallback dir.
        resDst = new InodeTree.ResolveResult<AbstractFileSystem>(
            InodeTree.ResultKind.INTERNAL_DIR,
            fsState.getRootFallbackLink().getTargetFileSystem(), "/",
            new Path(resDstWithLastComp.resolvedPath), false);
      } else {
        // The link resolved to some target fs or fallback fs.
        resDst = resDstWithLastComp;
      }
    }

    //Alternate 1: renames within same file system
    URI srcUri = resSrc.targetFileSystem.getUri();
    URI dstUri = resDst.targetFileSystem.getUri();
    ViewFileSystem.verifyRenameStrategy(srcUri, dstUri,
        resSrc.targetFileSystem == resDst.targetFileSystem, renameStrategy);

    ChRootedFs srcFS = (ChRootedFs) resSrc.targetFileSystem;
    ChRootedFs dstFS = (ChRootedFs) resDst.targetFileSystem;
    srcFS.getMyFs().renameInternal(srcFS.fullPath(resSrc.remainingPath),
        dstFS.fullPath(resDst.remainingPath), overwrite);
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
    // Prefix validated at mount time and rest of path validated by mount
    // target.
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

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    return res.targetFileSystem.createSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    res.targetFileSystem.renameSnapshot(res.remainingPath, snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    res.targetFileSystem.deleteSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void satisfyStoragePolicy(final Path path) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.satisfyStoragePolicy(res.remainingPath);
  }

  @Override
  public void setStoragePolicy(final Path path, final String policyName)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setStoragePolicy(res.remainingPath, policyName);
  }

  @Override
  public void unsetStoragePolicy(final Path src)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(src), true);
    res.targetFileSystem.unsetStoragePolicy(res.remainingPath);
  }

  /**
   * Retrieve the storage policy for a given file or directory.
   *
   * @param src file or directory path.
   * @return storage policy for give file.
   * @throws IOException
   */
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(src), true);
    return res.targetFileSystem.getStoragePolicy(res.remainingPath);
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
    private InodeTree<AbstractFileSystem> fsState;
    private Configuration conf;

    public InternalDirOfViewFs(final InodeTree.INodeDir<AbstractFileSystem> dir,
        final long cTime, final UserGroupInformation ugi, final URI uri,
        InodeTree fsState, Configuration conf)
      throws URISyntaxException {
      super(FsConstants.VIEWFS_URI, FsConstants.VIEWFS_SCHEME, false, -1);
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
      myUri = uri;
      this.fsState = fsState;
      this.conf = conf;
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
      Preconditions.checkNotNull(f, "File cannot be null.");
      if (InodeTree.SlashPath.equals(f)) {
        throw new FileAlreadyExistsException(
            "/ is not a file. The directory / already exist at: "
                + theInternalDir.fullPath);
      }

      if (this.fsState.getRootFallbackLink() != null) {
        if (theInternalDir.getChildren().containsKey(f.getName())) {
          throw new FileAlreadyExistsException(
              "A mount path(file/dir) already exist with the requested path: "
                  + theInternalDir.getChildren().get(f.getName()).fullPath);
        }

        AbstractFileSystem linkedFallbackFs =
            this.fsState.getRootFallbackLink().getTargetFileSystem();
        Path parent = Path.getPathWithoutSchemeAndAuthority(
            new Path(theInternalDir.fullPath));
        String leaf = f.getName();
        Path fileToCreate = new Path(parent, leaf);

        try {
          return linkedFallbackFs
              .createInternal(fileToCreate, flag, absolutePermission,
                  bufferSize, replication, blockSize, progress, checksumOpt,
                  true);
        } catch (IOException e) {
          StringBuilder msg =
              new StringBuilder("Failed to create file:").append(fileToCreate)
                  .append(" at fallback : ").append(linkedFallbackFs.getUri());
          LOG.error(msg.toString(), e);
          throw e;
        }
      }

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
      // When application calls listFiles on internalDir, it would return
      // RemoteIterator from InternalDirOfViewFs. If there is a fallBack, there
      // is a chance of files exists under that internalDir in fallback.
      // Iterator#next will call getFileBlockLocations with that files. So, we
      // should return getFileBlockLocations on fallback. See HDFS-15532.
      if (!InodeTree.SlashPath.equals(f) && this.fsState
          .getRootFallbackLink() != null) {
        AbstractFileSystem linkedFallbackFs =
            this.fsState.getRootFallbackLink().getTargetFileSystem();
        Path parent = Path.getPathWithoutSchemeAndAuthority(
            new Path(theInternalDir.fullPath));
        Path pathToFallbackFs = new Path(parent, f.getName());
        return linkedFallbackFs
            .getFileBlockLocations(pathToFallbackFs, start, len);
      }
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
          PERMISSION_555, ugi.getShortUserName(), ugi.getPrimaryGroupName(),
          new Path(theInternalDir.fullPath).makeQualified(
              myUri, null));
    }
    
    @Override
    public FileStatus getFileLinkStatus(final Path f)
        throws IOException {
      // look up i internalDirs children - ignore first Slash
      INode<AbstractFileSystem> inode =
          theInternalDir.getChildren().get(f.toUri().toString().substring(1));
      if (inode == null) {
        throw new FileNotFoundException(
            "viewFs internal mount table - missing entry:" + f);
      }
      FileStatus result;
      if (inode.isLink()) {
        INodeLink<AbstractFileSystem> inodelink = 
          (INodeLink<AbstractFileSystem>) inode;
        try {
          String linkedPath = inodelink.getTargetFileSystem()
              .getUri().getPath();
          FileStatus status = ((ChRootedFs)inodelink.getTargetFileSystem())
              .getMyFs().getFileStatus(new Path(linkedPath));
          result = new FileStatus(status.getLen(), false,
            status.getReplication(), status.getBlockSize(),
            status.getModificationTime(), status.getAccessTime(),
            status.getPermission(), status.getOwner(), status.getGroup(),
            inodelink.getTargetLink(),
            new Path(inode.fullPath).makeQualified(
                myUri, null));
        } catch (FileNotFoundException ex) {
          result = new FileStatus(0, false, 0, 0, creationTime, creationTime,
            PERMISSION_555, ugi.getShortUserName(), ugi.getPrimaryGroupName(),
            inodelink.getTargetLink(),
            new Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      } else {
        result = new FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_555, ugi.getShortUserName(), ugi.getPrimaryGroupName(),
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

    /**
     * {@inheritDoc}
     *
     * Note: listStatus on root("/") considers listing from fallbackLink if
     * available. If the same directory name is present in configured mount
     * path as well as in fallback link, then only the configured mount path
     * will be listed in the returned result.
     */
    @Override
    public FileStatus[] listStatus(final Path f) throws IOException {
      checkPathIsSlash(f);
      FileStatus[] fallbackStatuses = listStatusForFallbackLink();
      Set<FileStatus> linkStatuses = new HashSet<>();
      Set<FileStatus> internalDirStatuses = new HashSet<>();
      int i = 0;
      for (Entry<String, INode<AbstractFileSystem>> iEntry :
          theInternalDir.getChildren().entrySet()) {
        INode<AbstractFileSystem> inode = iEntry.getValue();
        Path path = new Path(inode.fullPath).makeQualified(myUri, null);
        if (inode.isLink()) {
          INodeLink<AbstractFileSystem> link = 
            (INodeLink<AbstractFileSystem>) inode;

          if (showMountLinksAsSymlinks) {
            // To maintain backward compatibility, with default option(showing
            // mount links as symlinks), we will represent target link as
            // symlink and rest other properties are belongs to mount link only.
            linkStatuses.add(
                new FileStatus(0, false, 0, 0, creationTime, creationTime,
                    PERMISSION_555, ugi.getShortUserName(),
                    ugi.getPrimaryGroupName(), link.getTargetLink(), path));
            continue;
          }

          //  We will represent as non-symlinks. Here it will show target
          //  directory/file properties like permissions, isDirectory etc on
          //  mount path. The path will be a mount link path and isDirectory is
          //  true if target is dir, otherwise false.
          String linkedPath = link.getTargetFileSystem().getUri().getPath();
          if ("".equals(linkedPath)) {
            linkedPath = "/";
          }
          try {
            FileStatus status =
                ((ChRootedFs) link.getTargetFileSystem()).getMyFs()
                    .getFileStatus(new Path(linkedPath));
            linkStatuses.add(
                new FileStatus(status.getLen(), status.isDirectory(),
                    status.getReplication(), status.getBlockSize(),
                    status.getModificationTime(), status.getAccessTime(),
                    status.getPermission(), status.getOwner(),
                    status.getGroup(), null, path));
          } catch (FileNotFoundException ex) {
            LOG.warn("Cannot get one of the children's(" + path
                + ")  target path(" + link.getTargetFileSystem().getUri()
                + ") file status.", ex);
            throw ex;
          }
        } else {
          internalDirStatuses.add(
              new FileStatus(0, true, 0, 0, creationTime, creationTime,
                  PERMISSION_555, ugi.getShortUserName(),
                  ugi.getPrimaryGroupName(), path));
        }
      }

      FileStatus[] internalDirStatusesMergedWithFallBack = internalDirStatuses
          .toArray(new FileStatus[internalDirStatuses.size()]);
      if (fallbackStatuses.length > 0) {
        internalDirStatusesMergedWithFallBack =
            merge(fallbackStatuses, internalDirStatusesMergedWithFallBack);
      }

      // Links will always have precedence than internalDir or fallback paths.
      return merge(linkStatuses.toArray(new FileStatus[linkStatuses.size()]),
          internalDirStatusesMergedWithFallBack);
    }

    private FileStatus[] merge(FileStatus[] toStatuses,
        FileStatus[] fromStatuses) {
      ArrayList<FileStatus> result = new ArrayList<>();
      Set<String> pathSet = new HashSet<>();
      for (FileStatus status : toStatuses) {
        result.add(status);
        pathSet.add(status.getPath().getName());
      }
      for (FileStatus status : fromStatuses) {
        if (!pathSet.contains(status.getPath().getName())) {
          result.add(status);
        }
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    private FileStatus[] listStatusForFallbackLink() throws IOException {
      if (fsState.getRootFallbackLink() != null) {
        AbstractFileSystem linkedFallbackFs =
            fsState.getRootFallbackLink().getTargetFileSystem();
        Path p = Path.getPathWithoutSchemeAndAuthority(
            new Path(theInternalDir.fullPath));
        if (theInternalDir.isRoot() || FileContext
            .getFileContext(linkedFallbackFs, conf).util().exists(p)) {
          // Fallback link is only applicable for root
          FileStatus[] statuses = linkedFallbackFs.listStatus(p);
          for (FileStatus status : statuses) {
            // Fix the path back to viewfs scheme
            Path pathFromConfiguredFallbackRoot =
                new Path(p, status.getPath().getName());
            status.setPath(
                new Path(myUri.toString(), pathFromConfiguredFallbackRoot));
          }
          return statuses;
        }
      }
      return new FileStatus[0];
    }

    @Override
    public void mkdir(final Path dir, final FsPermission permission,
        final boolean createParent) throws IOException {
      if (theInternalDir.isRoot() && dir == null) {
        throw new FileAlreadyExistsException("/ already exits");
      }

      if (this.fsState.getRootFallbackLink() != null) {
        AbstractFileSystem linkedFallbackFs =
            this.fsState.getRootFallbackLink().getTargetFileSystem();
        Path parent = Path.getPathWithoutSchemeAndAuthority(
            new Path(theInternalDir.fullPath));
        String leafChild = (InodeTree.SlashPath.equals(dir)) ?
            InodeTree.SlashPath.toString() :
            dir.getName();
        Path dirToCreate = new Path(parent, leafChild);
        try {
          // We are here because, the parent dir already exist in the mount
          // table internal tree. So, let's create parent always in fallback.
          linkedFallbackFs.mkdir(dirToCreate, permission, true);
          return;
        } catch (IOException e) {
          if (LOG.isDebugEnabled()) {
            StringBuilder msg = new StringBuilder("Failed to create {}")
                .append(" at fallback fs : {}");
            LOG.debug(msg.toString(), dirToCreate, linkedFallbackFs.getUri());
          }
          throw e;
        }
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
      return new AclStatus.Builder().owner(ugi.getShortUserName())
          .group(ugi.getPrimaryGroupName())
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

    @Override
    public Path createSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("createSnapshot", path);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
        String snapshotNewName) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("renameSnapshot", path);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("deleteSnapshot", path);
    }

    @Override
    public void satisfyStoragePolicy(final Path path) throws IOException {
      throw readOnlyMountTable("satisfyStoragePolicy", path);
    }

    @Override
    public void setStoragePolicy(Path path, String policyName)
        throws IOException {
      throw readOnlyMountTable("setStoragePolicy", path);
    }
  }
}
