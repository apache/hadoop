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
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InodeTree implements a mount-table as a tree of inodes.
 * It is used to implement ViewFs and ViewFileSystem.
 * In order to use it the caller must subclass it and implement
 * the abstract methods {@link #getTargetFileSystem(INodeDir)}, etc.
 *
 * The mountable is initialized from the config variables as
 * specified in {@link ViewFs}
 *
 * @param <T> is AbstractFileSystem or FileSystem
 *
 * The two main methods are
 * {@link #InodeTree(Configuration, String)} // constructor
 * {@link #resolve(String, boolean)}
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract class InodeTree<T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InodeTree.class.getName());

  enum ResultKind {
    INTERNAL_DIR,
    EXTERNAL_DIR
  }

  static final Path SlashPath = new Path("/");
  // the root of the mount table
  private final INode<T> root;
  // the fallback filesystem
  private INodeLink<T> rootFallbackLink;
  // the homedir for this mount table
  private final String homedirPrefix;
  private List<MountPoint<T>> mountPoints = new ArrayList<MountPoint<T>>();
  private List<RegexMountPoint<T>> regexMountPointList =
      new ArrayList<RegexMountPoint<T>>();

  static class MountPoint<T> {
    String src;
    INodeLink<T> target;

    MountPoint(String srcPath, INodeLink<T> mountLink) {
      src = srcPath;
      target = mountLink;
    }
  }

  /**
   * Breaks file path into component names.
   * @param path
   * @return array of names component names
   */
  static String[] breakIntoPathComponents(final String path) {
    return path == null ? null : path.split(Path.SEPARATOR);
  }

  /**
   * Internal class for INode tree.
   * @param <T>
   */
  abstract static class INode<T> {
    final String fullPath; // the full path to the root

    public INode(String pathToNode, UserGroupInformation aUgi) {
      fullPath = pathToNode;
    }

    // INode forming the internal mount table directory tree
    // for ViewFileSystem. This internal directory tree is
    // constructed based on the mount table config entries
    // and is read only.
    abstract boolean isInternalDir();

    // INode linking to another filesystem. Represented
    // via mount table link config entries.
    boolean isLink() {
      return !isInternalDir();
    }
  }

  /**
   * Internal class to represent an internal dir of the mount table.
   * @param <T>
   */
  static class INodeDir<T> extends INode<T> {
    private final Map<String, INode<T>> children = new HashMap<>();
    private T internalDirFs = null; //filesystem of this internal directory
    private boolean isRoot = false;
    private INodeLink<T> fallbackLink = null;

    INodeDir(final String pathToNode, final UserGroupInformation aUgi) {
      super(pathToNode, aUgi);
    }

    @Override
    boolean isInternalDir() {
      return true;
    }

    T getInternalDirFs() {
      return internalDirFs;
    }

    void setInternalDirFs(T internalDirFs) {
      this.internalDirFs = internalDirFs;
    }

    void setRoot(boolean root) {
      isRoot = root;
    }

    boolean isRoot() {
      return isRoot;
    }

    INodeLink<T> getFallbackLink() {
      return fallbackLink;
    }

    void addFallbackLink(INodeLink<T> link) throws IOException {
      if (!isRoot) {
        throw new IOException("Fallback link can only be added for root");
      }
      this.fallbackLink = link;
    }

    Map<String, INode<T>> getChildren() {
      return Collections.unmodifiableMap(children);
    }

    INode<T> resolveInternal(final String pathComponent) {
      return children.get(pathComponent);
    }

    INodeDir<T> addDir(final String pathComponent,
        final UserGroupInformation aUgi) throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new FileAlreadyExistsException();
      }
      final INodeDir<T> newDir = new INodeDir<T>(fullPath +
          (isRoot() ? "" : "/") + pathComponent, aUgi);
      children.put(pathComponent, newDir);
      return newDir;
    }

    void addLink(final String pathComponent, final INodeLink<T> link)
        throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new FileAlreadyExistsException();
      }
      children.put(pathComponent, link);
    }
  }

  /**
   * Mount table link type.
   */
  enum LinkType {
    /**
     * Link entry pointing to a single filesystem uri.
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.link.<link_name>
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK}
     */
    SINGLE,
    /**
     * Fallback filesystem for the paths not mounted by
     * any single link entries.
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.linkFallback
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK_FALLBACK}
     */
    SINGLE_FALLBACK,
    /**
     * Link entry pointing to an union of two or more filesystem uris.
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.linkMerge.<link_name>
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK_MERGE}
     */
    MERGE,
    /**
     * Link entry for merging mount table's root with the
     * root of another filesystem.
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.linkMergeSlash
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK_MERGE_SLASH}
     */
    MERGE_SLASH,
    /**
     * Link entry to write to multiple filesystems and read
     * from the closest filesystem.
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.linkNfly
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK_NFLY}
     */
    NFLY,
    /**
     * Link entry which source are regex exrepssions and target refer matched
     * group from source
     * Config prefix: fs.viewfs.mounttable.<mnt_tbl_name>.linkRegex
     * Refer: {@link Constants#CONFIG_VIEWFS_LINK_REGEX}
     */
    REGEX;
  }

  /**
   * An internal class to represent a mount link.
   * A mount link can be single dir link or a merge dir link.

   * A merge dir link is  a merge (junction) of links to dirs:
   * example : merge of 2 dirs
   *     /users -> hdfs:nn1//users
   *     /users -> hdfs:nn2//users
   *
   * For a merge, each target is checked to be dir when created but if target
   * is changed later it is then ignored (a dir with null entries)
   */
  static class INodeLink<T> extends INode<T> {
    final URI[] targetDirLinkList;
    final T targetFileSystem;   // file system object created from the link.

    /**
     * Construct a mergeLink or nfly.
     */
    INodeLink(final String pathToNode, final UserGroupInformation aUgi,
        final T targetMergeFs, final URI[] aTargetDirLinkList) {
      super(pathToNode, aUgi);
      targetFileSystem = targetMergeFs;
      targetDirLinkList = aTargetDirLinkList;
    }

    /**
     * Construct a simple link (i.e. not a mergeLink).
     */
    INodeLink(final String pathToNode, final UserGroupInformation aUgi,
        final T targetFs, final URI aTargetDirLink) {
      super(pathToNode, aUgi);
      targetFileSystem = targetFs;
      targetDirLinkList = new URI[1];
      targetDirLinkList[0] = aTargetDirLink;
    }

    /**
     * Get the target of the link. If a merge link then it returned
     * as "," separated URI list.
     */
    Path getTargetLink() {
      StringBuilder result = new StringBuilder(targetDirLinkList[0].toString());
      // If merge link, use "," as separator between the merged URIs
      for (int i = 1; i < targetDirLinkList.length; ++i) {
        result.append(',').append(targetDirLinkList[i].toString());
      }
      return new Path(result.toString());
    }

    @Override
    boolean isInternalDir() {
      return false;
    }

    public T getTargetFileSystem() {
      return targetFileSystem;
    }
  }

  private void createLink(final String src, final String target,
      final LinkType linkType, final String settings,
      final UserGroupInformation aUgi,
      final Configuration config)
      throws URISyntaxException, IOException,
      FileAlreadyExistsException, UnsupportedFileSystemException {
    // Validate that src is valid absolute path
    final Path srcPath = new Path(src);
    if (!srcPath.isAbsoluteAndSchemeAuthorityNull()) {
      throw new IOException("ViewFs: Non absolute mount name in config:" + src);
    }

    final String[] srcPaths = breakIntoPathComponents(src);
    // Make sure root is of INodeDir type before
    // adding any regular links to it.
    Preconditions.checkState(root.isInternalDir());
    INodeDir<T> curInode = getRootDir();
    int i;
    // Ignore first initial slash, process all except last component
    for (i = 1; i < srcPaths.length - 1; i++) {
      final String iPath = srcPaths[i];
      INode<T> nextInode = curInode.resolveInternal(iPath);
      if (nextInode == null) {
        INodeDir<T> newDir = curInode.addDir(iPath, aUgi);
        newDir.setInternalDirFs(getTargetFileSystem(newDir));
        nextInode = newDir;
      }
      if (nextInode.isLink()) {
        // Error - expected a dir but got a link
        throw new FileAlreadyExistsException("Path " + nextInode.fullPath +
            " already exists as link");
      } else {
        assert(nextInode.isInternalDir());
        curInode = (INodeDir<T>) nextInode;
      }
    }

    // Now process the last component
    // Add the link in 2 cases: does not exist or a link exists
    String iPath = srcPaths[i];// last component
    if (curInode.resolveInternal(iPath) != null) {
      //  directory/link already exists
      StringBuilder strB = new StringBuilder(srcPaths[0]);
      for (int j = 1; j <= i; ++j) {
        strB.append('/').append(srcPaths[j]);
      }
      throw new FileAlreadyExistsException("Path " + strB +
          " already exists as dir; cannot create link here");
    }

    final INodeLink<T> newLink;
    final String fullPath = curInode.fullPath + (curInode == root ? "" : "/")
        + iPath;
    switch (linkType) {
    case SINGLE:
      newLink = new INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(new URI(target)), new URI(target));
      break;
    case SINGLE_FALLBACK:
    case MERGE_SLASH:
      // Link fallback and link merge slash configuration
      // are handled specially at InodeTree.
      throw new IllegalArgumentException("Unexpected linkType: " + linkType);
    case MERGE:
    case NFLY:
      final URI[] targetUris = StringUtils.stringToURI(
          StringUtils.getStrings(target));
      newLink = new INodeLink<T>(fullPath, aUgi,
            getTargetFileSystem(settings, targetUris), targetUris);
      break;
    default:
      throw new IllegalArgumentException(linkType + ": Infeasible linkType");
    }
    curInode.addLink(iPath, newLink);
    mountPoints.add(new MountPoint<T>(src, newLink));
  }

  /**
   * The user of this class must subclass and implement the following
   * 3 abstract methods.
   * @throws IOException
   */
  protected abstract T getTargetFileSystem(URI uri)
      throws UnsupportedFileSystemException, URISyntaxException, IOException;

  protected abstract T getTargetFileSystem(INodeDir<T> dir)
      throws URISyntaxException, IOException;

  protected abstract T getTargetFileSystem(String settings, URI[] mergeFsURIs)
      throws UnsupportedFileSystemException, URISyntaxException, IOException;

  private INodeDir<T> getRootDir() {
    Preconditions.checkState(root.isInternalDir());
    return (INodeDir<T>)root;
  }

  private INodeLink<T> getRootLink() {
    Preconditions.checkState(root.isLink());
    return (INodeLink<T>)root;
  }

  private boolean hasFallbackLink() {
    return rootFallbackLink != null;
  }

  /**
   * @return true if the root represented as internalDir. In LinkMergeSlash,
   * there will be root to root mapping. So, root does not represent as
   * internalDir.
   */
  protected boolean isRootInternalDir() {
    return root.isInternalDir();
  }

  protected INodeLink<T> getRootFallbackLink() {
    Preconditions.checkState(root.isInternalDir());
    return rootFallbackLink;
  }

  /**
   * An internal class representing the ViewFileSystem mount table
   * link entries and their attributes.
   * @see LinkType
   */
  private static class LinkEntry {
    private final String src;
    private final String target;
    private final LinkType linkType;
    private final String settings;
    private final UserGroupInformation ugi;
    private final Configuration config;

    LinkEntry(String src, String target, LinkType linkType, String settings,
        UserGroupInformation ugi, Configuration config) {
      this.src = src;
      this.target = target;
      this.linkType = linkType;
      this.settings = settings;
      this.ugi = ugi;
      this.config = config;
    }

    String getSrc() {
      return src;
    }

    String getTarget() {
      return target;
    }

    LinkType getLinkType() {
      return linkType;
    }

    boolean isLinkType(LinkType type) {
      return this.linkType == type;
    }

    String getSettings() {
      return settings;
    }

    UserGroupInformation getUgi() {
      return ugi;
    }

    Configuration getConfig() {
      return config;
    }
  }

  /**
   * Create Inode Tree from the specified mount-table specified in Config
   * @param config - the mount table keys are prefixed with
   *       FsConstants.CONFIG_VIEWFS_PREFIX
   * @param viewName - the name of the mount table - if null use defaultMT name
   * @throws UnsupportedFileSystemException
   * @throws URISyntaxException
   * @throws FileAlreadyExistsException
   * @throws IOException
   */
  protected InodeTree(final Configuration config, final String viewName,
      final URI theUri, boolean initingUriAsFallbackOnNoMounts)
      throws UnsupportedFileSystemException, URISyntaxException,
      FileAlreadyExistsException, IOException {
    String mountTableName = viewName;
    if (mountTableName == null) {
      mountTableName = ConfigUtil.getDefaultMountTableName(config);
    }
    homedirPrefix = ConfigUtil.getHomeDirValue(config, mountTableName);

    boolean isMergeSlashConfigured = false;
    String mergeSlashTarget = null;
    List<LinkEntry> linkEntries = new LinkedList<>();

    final String mountTablePrefix =
        Constants.CONFIG_VIEWFS_PREFIX + "." + mountTableName + ".";
    final String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final String linkFallbackPrefix = Constants.CONFIG_VIEWFS_LINK_FALLBACK;
    final String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    final String linkMergeSlashPrefix =
        Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH;
    boolean gotMountTableEntry = false;
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    for (Entry<String, String> si : config) {
      final String key = si.getKey();
      if (!key.startsWith(mountTablePrefix)) {
        continue;
      }

      gotMountTableEntry = true;
      LinkType linkType;
      String src = key.substring(mountTablePrefix.length());
      String settings = null;
      if (src.startsWith(linkPrefix)) {
        src = src.substring(linkPrefix.length());
        if (src.equals(SlashPath.toString())) {
          throw new UnsupportedFileSystemException("Unexpected mount table "
              + "link entry '" + key + "'. Use "
              + Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH  + " instead!");
        }
        linkType = LinkType.SINGLE;
      } else if (src.startsWith(linkFallbackPrefix)) {
        checkMntEntryKeyEqualsTarget(src, linkFallbackPrefix);
        linkType = LinkType.SINGLE_FALLBACK;
      } else if (src.startsWith(linkMergePrefix)) { // A merge link
        src = src.substring(linkMergePrefix.length());
        linkType = LinkType.MERGE;
      } else if (src.startsWith(linkMergeSlashPrefix)) {
        // This is a LinkMergeSlash entry. This entry should
        // not have any additional source path.
        checkMntEntryKeyEqualsTarget(src, linkMergeSlashPrefix);
        linkType = LinkType.MERGE_SLASH;
      } else if (src.startsWith(Constants.CONFIG_VIEWFS_LINK_NFLY)) {
        // prefix.settings.src
        src = src.substring(Constants.CONFIG_VIEWFS_LINK_NFLY.length() + 1);
        // settings.src
        settings = src.substring(0, src.indexOf('.'));
        // settings

        // settings.src
        src = src.substring(settings.length() + 1);
        // src

        linkType = LinkType.NFLY;
      } else if (src.startsWith(Constants.CONFIG_VIEWFS_LINK_REGEX)) {
        linkEntries.add(
            buildLinkRegexEntry(config, ugi, src, si.getValue()));
        continue;
      } else if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
        // ignore - we set home dir from config
        continue;
      } else {
        throw new IOException("ViewFs: Cannot initialize: Invalid entry in " +
            "Mount table in config: " + src);
      }

      final String target = si.getValue();
      if (linkType != LinkType.MERGE_SLASH) {
        if (isMergeSlashConfigured) {
          throw new IOException("Mount table " + mountTableName
              + " has already been configured with a merge slash link. "
              + "A regular link should not be added.");
        }
        linkEntries.add(
            new LinkEntry(src, target, linkType, settings, ugi, config));
      } else {
        if (!linkEntries.isEmpty()) {
          throw new IOException("Mount table " + mountTableName
              + " has already been configured with regular links. "
              + "A merge slash link should not be configured.");
        }
        if (isMergeSlashConfigured) {
          throw new IOException("Mount table " + mountTableName
              + " has already been configured with a merge slash link. "
              + "Multiple merge slash links for the same mount table is "
              + "not allowed.");
        }
        isMergeSlashConfigured = true;
        mergeSlashTarget = target;
      }
    } // End of for loop.

    if (isMergeSlashConfigured) {
      Preconditions.checkNotNull(mergeSlashTarget);
      root = new INodeLink<T>(mountTableName, ugi,
          getTargetFileSystem(new URI(mergeSlashTarget)),
          new URI(mergeSlashTarget));
      mountPoints.add(new MountPoint<T>("/", (INodeLink<T>) root));
      rootFallbackLink = null;
    } else {
      root = new INodeDir<T>("/", UserGroupInformation.getCurrentUser());
      getRootDir().setInternalDirFs(getTargetFileSystem(getRootDir()));
      getRootDir().setRoot(true);
      INodeLink<T> fallbackLink = null;
      for (LinkEntry le : linkEntries) {
        switch (le.getLinkType()) {
        case SINGLE_FALLBACK:
          if (fallbackLink != null) {
            throw new IOException("Mount table " + mountTableName
                + " has already been configured with a link fallback. "
                + "Multiple fallback links for the same mount table is "
                + "not allowed.");
          }
          fallbackLink = new INodeLink<T>(mountTableName, ugi,
              getTargetFileSystem(new URI(le.getTarget())),
              new URI(le.getTarget()));
          continue;
        case REGEX:
          addRegexMountEntry(le);
          continue;
        default:
          createLink(le.getSrc(), le.getTarget(), le.getLinkType(),
              le.getSettings(), le.getUgi(), le.getConfig());
        }
      }
      rootFallbackLink = fallbackLink;
      getRootDir().addFallbackLink(rootFallbackLink);
    }

    if (!gotMountTableEntry) {
      if (!initingUriAsFallbackOnNoMounts) {
        throw new IOException(new StringBuilder(
            "ViewFs: Cannot initialize: Empty Mount table in config for ")
            .append(theUri.getScheme()).append("://").append(mountTableName)
            .append("/").toString());
      }
      StringBuilder msg =
          new StringBuilder("Empty mount table detected for ").append(theUri)
              .append(" and considering itself as a linkFallback.");
      FileSystem.LOG.info(msg.toString());
      rootFallbackLink =
          new INodeLink<T>(mountTableName, ugi, getTargetFileSystem(theUri),
              theUri);
      getRootDir().addFallbackLink(rootFallbackLink);
    }
  }

  private void checkMntEntryKeyEqualsTarget(
      String mntEntryKey, String targetMntEntryKey) throws IOException {
    if (!mntEntryKey.equals(targetMntEntryKey)) {
      throw new IOException("ViewFs: Mount points initialization error." +
          " Invalid " + targetMntEntryKey +
          " entry in config: " + mntEntryKey);
    }
  }

  private void addRegexMountEntry(LinkEntry le) throws IOException {
    LOGGER.info("Add regex mount point:" + le.getSrc()
        + ", target:" + le.getTarget()
        + ", interceptor settings:" + le.getSettings());
    RegexMountPoint regexMountPoint =
        new RegexMountPoint<T>(
            this, le.getSrc(), le.getTarget(), le.getSettings());
    regexMountPoint.initialize();
    regexMountPointList.add(regexMountPoint);
  }

  private LinkEntry buildLinkRegexEntry(
      Configuration config, UserGroupInformation ugi,
      String mntEntryStrippedKey, String mntEntryValue) {
    String linkKeyPath = null;
    String settings = null;
    final String linkRegexPrefix = Constants.CONFIG_VIEWFS_LINK_REGEX + ".";
    // settings#.linkKey
    String settingsAndLinkKeyPath =
        mntEntryStrippedKey.substring(linkRegexPrefix.length());
    int settingLinkKeySepIndex = settingsAndLinkKeyPath
        .indexOf(RegexMountPoint.SETTING_SRCREGEX_SEP);
    if (settingLinkKeySepIndex == -1) {
      // There's no settings
      linkKeyPath = settingsAndLinkKeyPath;
      settings = null;
    } else {
      // settings#.linkKey style configuration
      // settings from settings#.linkKey
      settings =
          settingsAndLinkKeyPath.substring(0, settingLinkKeySepIndex);
      // linkKeyPath
      linkKeyPath = settingsAndLinkKeyPath.substring(
          settings.length() + RegexMountPoint.SETTING_SRCREGEX_SEP
              .length());
    }
    return new LinkEntry(
        linkKeyPath, mntEntryValue, LinkType.REGEX, settings, ugi, config);
  }

  /**
   * Resolve returns ResolveResult.
   * The caller can continue the resolution of the remainingPath
   * in the targetFileSystem.
   *
   * If the input pathname leads to link to another file system then
   * the targetFileSystem is the one denoted by the link (except it is
   * file system chrooted to link target.
   * If the input pathname leads to an internal mount-table entry then
   * the target file system is one that represents the internal inode.
   */
  static class ResolveResult<T> {
    final ResultKind kind;
    final T targetFileSystem;
    final String resolvedPath;
    final Path remainingPath;   // to resolve in the target FileSystem
    private final boolean isLastInternalDirLink;

    ResolveResult(final ResultKind k, final T targetFs, final String resolveP,
        final Path remainingP, boolean isLastIntenalDirLink) {
      kind = k;
      targetFileSystem = targetFs;
      resolvedPath = resolveP;
      remainingPath = remainingP;
      this.isLastInternalDirLink = isLastIntenalDirLink;
    }

    // Internal dir path resolution completed within the mount table
    boolean isInternalDir() {
      return (kind == ResultKind.INTERNAL_DIR);
    }

    // Indicates whether the internal dir path resolution completed at the link
    // or resolved due to fallback.
    boolean isLastInternalDirLink() {
      return this.isLastInternalDirLink;
    }
  }

  /**
   * Resolve the pathname p relative to root InodeDir.
   * @param p - input path
   * @param resolveLastComponent
   * @return ResolveResult which allows further resolution of the remaining path
   * @throws FileNotFoundException
   */
  ResolveResult<T> resolve(final String p, final boolean resolveLastComponent)
      throws FileNotFoundException {
    ResolveResult<T> resolveResult = null;
    String[] path = breakIntoPathComponents(p);
    if (path.length <= 1) { // special case for when path is "/"
      T targetFs = root.isInternalDir() ?
          getRootDir().getInternalDirFs()
          : getRootLink().getTargetFileSystem();
      resolveResult = new ResolveResult<T>(ResultKind.INTERNAL_DIR,
          targetFs, root.fullPath, SlashPath, false);
      return resolveResult;
    }

    /**
     * linkMergeSlash has been configured. The root of this mount table has
     * been linked to the root directory of a file system.
     * The first non-slash path component should be name of the mount table.
     */
    if (root.isLink()) {
      Path remainingPath;
      StringBuilder remainingPathStr = new StringBuilder();
      // ignore first slash
      for (int i = 1; i < path.length; i++) {
        remainingPathStr.append("/").append(path[i]);
      }
      remainingPath = new Path(remainingPathStr.toString());
      resolveResult = new ResolveResult<T>(ResultKind.EXTERNAL_DIR,
          getRootLink().getTargetFileSystem(), root.fullPath, remainingPath,
          true);
      return resolveResult;
    }
    Preconditions.checkState(root.isInternalDir());
    INodeDir<T> curInode = getRootDir();

    // Try to resolve path in the regex mount point
    resolveResult = tryResolveInRegexMountpoint(p, resolveLastComponent);
    if (resolveResult != null) {
      return resolveResult;
    }

    int i;
    // ignore first slash
    for (i = 1; i < path.length - (resolveLastComponent ? 0 : 1); i++) {
      INode<T> nextInode = curInode.resolveInternal(path[i]);
      if (nextInode == null) {
        if (hasFallbackLink()) {
          resolveResult = new ResolveResult<T>(ResultKind.EXTERNAL_DIR,
              getRootFallbackLink().getTargetFileSystem(), root.fullPath,
              new Path(p), false);
          return resolveResult;
        } else {
          StringBuilder failedAt = new StringBuilder(path[0]);
          for (int j = 1; j <= i; ++j) {
            failedAt.append('/').append(path[j]);
          }
          throw (new FileNotFoundException(
              "File/Directory does not exist: " + failedAt.toString()));
        }
      }

      if (nextInode.isLink()) {
        final INodeLink<T> link = (INodeLink<T>) nextInode;
        final Path remainingPath;
        if (i >= path.length - 1) {
          remainingPath = SlashPath;
        } else {
          StringBuilder remainingPathStr =
              new StringBuilder("/" + path[i + 1]);
          for (int j = i + 2; j < path.length; ++j) {
            remainingPathStr.append('/').append(path[j]);
          }
          remainingPath = new Path(remainingPathStr.toString());
        }
        resolveResult = new ResolveResult<T>(ResultKind.EXTERNAL_DIR,
            link.getTargetFileSystem(), nextInode.fullPath, remainingPath,
            true);
        return resolveResult;
      } else if (nextInode.isInternalDir()) {
        curInode = (INodeDir<T>) nextInode;
      }
    }

    // We have resolved to an internal dir in mount table.
    Path remainingPath;
    if (resolveLastComponent) {
      remainingPath = SlashPath;
    } else {
      // note we have taken care of when path is "/" above
      // for internal dirs rem-path does not start with / since the lookup
      // that follows will do a children.get(remaningPath) and will have to
      // strip-out the initial /
      StringBuilder remainingPathStr = new StringBuilder("/" + path[i]);
      for (int j = i + 1; j < path.length; ++j) {
        remainingPathStr.append('/').append(path[j]);
      }
      remainingPath = new Path(remainingPathStr.toString());
    }
    resolveResult = new ResolveResult<T>(ResultKind.INTERNAL_DIR,
        curInode.getInternalDirFs(), curInode.fullPath, remainingPath, false);
    return resolveResult;
  }

  /**
   * Walk through all regex mount points to see
   * whether the path match any regex expressions.
   *  E.g. link: ^/user/(?<username>\\w+) => s3://$user.apache.com/_${user}
   *  srcPath: is /user/hadoop/dir1
   *  resolveLastComponent: true
   *  then return value is s3://hadoop.apache.com/_hadoop
   *
   * @param srcPath
   * @param resolveLastComponent
   * @return
   */
  protected ResolveResult<T> tryResolveInRegexMountpoint(final String srcPath,
      final boolean resolveLastComponent) {
    for (RegexMountPoint regexMountPoint : regexMountPointList) {
      ResolveResult resolveResult =
          regexMountPoint.resolve(srcPath, resolveLastComponent);
      if (resolveResult != null) {
        return resolveResult;
      }
    }
    return null;
  }

  /**
   * Build resolve result.
   * Here's an example
   * Mountpoint: fs.viewfs.mounttable.mt
   *     .linkRegex.replaceresolveddstpath:_:-#.^/user/(?<username>\w+)
   * Value: /targetTestRoot/$username
   * Dir path to test:
   * viewfs://mt/user/hadoop_user1/hadoop_dir1
   * Expect path: /targetTestRoot/hadoop-user1/hadoop_dir1
   * resolvedPathStr: /user/hadoop_user1
   * targetOfResolvedPathStr: /targetTestRoot/hadoop-user1
   * remainingPath: /hadoop_dir1
   *
   * @return targetFileSystem or null on exceptions.
   */
  protected ResolveResult<T> buildResolveResultForRegexMountPoint(
      ResultKind resultKind, String resolvedPathStr,
      String targetOfResolvedPathStr, Path remainingPath) {
    try {
      T targetFs = getTargetFileSystem(
          new URI(targetOfResolvedPathStr));
      return new ResolveResult<T>(resultKind, targetFs, resolvedPathStr,
          remainingPath, true);
    } catch (IOException ex) {
      LOGGER.error(String.format(
          "Got Exception while build resolve result."
              + " ResultKind:%s, resolvedPathStr:%s,"
              + " targetOfResolvedPathStr:%s, remainingPath:%s,"
              + " will return null.",
          resultKind, resolvedPathStr, targetOfResolvedPathStr, remainingPath),
          ex);
      return null;
    } catch (URISyntaxException uex) {
      LOGGER.error(String.format(
          "Got Exception while build resolve result."
              + " ResultKind:%s, resolvedPathStr:%s,"
              + " targetOfResolvedPathStr:%s, remainingPath:%s,"
              + " will return null.",
          resultKind, resolvedPathStr, targetOfResolvedPathStr, remainingPath),
          uex);
      return null;
    }
  }

  List<MountPoint<T>> getMountPoints() {
    return mountPoints;
  }

  /**
   *
   * @return home dir value from mount table; null if no config value
   * was found.
   */
  String getHomeDirPrefixValue() {
    return homedirPrefix;
  }
}
