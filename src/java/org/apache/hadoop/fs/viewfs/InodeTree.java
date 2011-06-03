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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;


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
 * The three main methods are
 * {@link #InodeTreel(Configuration)} // constructor
 * {@link #InodeTree(Configuration, String)} // constructor
 * {@link #resolve(String, boolean)} 
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable 
abstract class InodeTree<T> {
  static enum ResultKind {isInternalDir, isExternalDir;};
  static final Path SlashPath = new Path("/");
  
  final INodeDir<T> root; // the root of the mount table
  
  final String homedirPrefix; // the homedir config value for this mount table
  
  List<MountPoint<T>> mountPoints = new ArrayList<MountPoint<T>>();
  
  
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
   * Internal class for inode tree
   * @param <T>
   */
  abstract static class INode<T> {
    final String fullPath; // the full path to the root
    public INode(String pathToNode, UserGroupInformation aUgi) {
      fullPath = pathToNode;
    }
  };

  /**
   * Internal class to represent an internal dir of the mount table
   * @param <T>
   */
  static class INodeDir<T> extends INode<T> {
    final Map<String,INode<T>> children = new HashMap<String,INode<T>>();
    T InodeDirFs =  null; // file system of this internal directory of mountT
    boolean isRoot = false;
    
    INodeDir(final String pathToNode, final UserGroupInformation aUgi) {
      super(pathToNode, aUgi);
    }

    INode<T> resolve(final String pathComponent) throws FileNotFoundException {
      final INode<T> result = resolveInternal(pathComponent);
      if (result == null) {
        throw new FileNotFoundException();
      }
      return result;
    }
    
    INode<T> resolveInternal(final String pathComponent)
        throws FileNotFoundException {
      return children.get(pathComponent);
    }
    
    INodeDir<T> addDir(final String pathComponent,
        final UserGroupInformation aUgi)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new FileAlreadyExistsException();
      }
      final INodeDir<T> newDir = new INodeDir<T>(fullPath+ (isRoot ? "" : "/") + 
          pathComponent, aUgi);
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
   * In internal class to represent a mount link
   * A mount link can be single dir link or a merge dir link.

   * A merge dir link is  a merge (junction) of links to dirs:
   * example : <merge of 2 dirs
   *     /users -> hdfs:nn1//users
   *     /users -> hdfs:nn2//users
   * 
   * For a merge, each target is checked to be dir when created but if target
   * is changed later it is then ignored (a dir with null entries)
   */
  static class INodeLink<T> extends INode<T> {
    final boolean isMergeLink; // true if MergeLink
    final URI[] targetDirLinkList;
    final T targetFileSystem;   // file system object created from the link.
    
    /**
     * Construct a mergeLink
     */
    INodeLink(final String pathToNode, final UserGroupInformation aUgi,
        final T targetMergeFs, final URI[] aTargetDirLinkList) {
      super(pathToNode, aUgi);
      targetFileSystem = targetMergeFs;
      targetDirLinkList = aTargetDirLinkList;
      isMergeLink = true;
    }
    
    /**
     * Construct a simple link (i.e. not a mergeLink)
     */
    INodeLink(final String pathToNode, final UserGroupInformation aUgi,
        final T targetFs, final URI aTargetDirLink) {
      super(pathToNode, aUgi);
      targetFileSystem = targetFs;
      targetDirLinkList = new URI[1];
      targetDirLinkList[0] = aTargetDirLink;
      isMergeLink = false;
    }
    
    /**
     * Get the target of the link
     * If a merge link then it returned as "," separated URI list.
     */
    Path getTargetLink() {
      // is merge link - use "," as separator between the merged URIs
      //String result = targetDirLinkList[0].toString();
      StringBuilder result = new StringBuilder(targetDirLinkList[0].toString());
      for (int i=1; i < targetDirLinkList.length; ++i) { 
        result.append(',').append(targetDirLinkList[i].toString());
      }
      return new Path(result.toString());
    }
  }


  private void createLink(final String src, final String target,
      final boolean isLinkMerge, final UserGroupInformation aUgi)
      throws URISyntaxException, IOException,
    FileAlreadyExistsException, UnsupportedFileSystemException {
    // Validate that src is valid absolute path
    final Path srcPath = new Path(src); 
    if (!srcPath.isAbsoluteAndSchemeAuthorityNull()) {
      throw new IOException("ViewFs:Non absolute mount name in config:" + src);
    }
 
    final String[] srcPaths = breakIntoPathComponents(src);
    INodeDir<T> curInode = root;
    int i;
    // Ignore first initial slash, process all except last component
    for (i = 1; i < srcPaths.length-1; i++) {
      final String iPath = srcPaths[i];
      INode<T> nextInode = curInode.resolveInternal(iPath);
      if (nextInode == null) {
        INodeDir<T> newDir = curInode.addDir(iPath, aUgi);
        newDir.InodeDirFs = getTargetFileSystem(newDir);
        nextInode = newDir;
      }
      if (nextInode instanceof INodeLink) {
        // Error - expected a dir but got a link
        throw new FileAlreadyExistsException("Path " + nextInode.fullPath +
            " already exists as link");
      } else {
        assert(nextInode instanceof INodeDir);
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
    if (isLinkMerge) { // Target is list of URIs
      String[] targetsList = StringUtils.getStrings(target);
      URI[] targetsListURI = new URI[targetsList.length];
      int k = 0;
      for (String itarget : targetsList) {
        targetsListURI[k++] = new URI(itarget);
      }
      newLink = new INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(targetsListURI), targetsListURI);
    } else {
      newLink = new INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(new URI(target)), new URI(target));
    }
    curInode.addLink(iPath, newLink);
    mountPoints.add(new MountPoint<T>(src, newLink));
  }
  
  /**
   * Below the "public" methods of InodeTree
   */
  
  /**
   * The user of this class must subclass and implement the following
   * 3 abstract methods.
   * @throws IOException 
   */
  protected abstract T getTargetFileSystem(final URI uri)
    throws UnsupportedFileSystemException, URISyntaxException, IOException;
  
  protected abstract T getTargetFileSystem(final INodeDir<T> dir)
    throws URISyntaxException;
  
  protected abstract T getTargetFileSystem(final URI[] mergeFsURIList)
  throws UnsupportedFileSystemException, URISyntaxException;
  
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
  protected InodeTree(final Configuration config, final String viewName)
      throws UnsupportedFileSystemException, URISyntaxException,
    FileAlreadyExistsException, IOException { 
    String vName = viewName;
    if (vName == null) {
      vName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    }
    homedirPrefix = ConfigUtil.getHomeDirValue(config, vName);
    root = new INodeDir<T>("/", UserGroupInformation.getCurrentUser());
    root.InodeDirFs = getTargetFileSystem(root);
    root.isRoot = true;
    
    final String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + 
                            vName + ".";
    final String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    boolean gotMountTableEntry = false;
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    for (Entry<String, String> si : config) {
      final String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        gotMountTableEntry = true;
        boolean isMergeLink = false;
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(linkPrefix)) {
          src = src.substring(linkPrefix.length());
        } else if (src.startsWith(linkMergePrefix)) { // A merge link
          isMergeLink = true;
          src = src.substring(linkMergePrefix.length());
        } else if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else {
          throw new IOException(
          "ViewFs: Cannot initialize: Invalid entry in Mount table in config: "+ 
          src);
        }
        final String target = si.getValue(); // link or merge link
        createLink(src, target, isMergeLink, ugi); 
      }
    }
    if (!gotMountTableEntry) {
      throw new IOException(
          "ViewFs: Cannot initialize: Empty Mount table in config for " + 
             vName == null ? "viewfs:///" : ("viewfs://" + vName + "/"));
    }
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
    
    ResolveResult(final ResultKind k, final T targetFs, final String resolveP,
        final Path remainingP) {
      kind = k;
      targetFileSystem = targetFs;
      resolvedPath = resolveP;
      remainingPath = remainingP;  
    }
    
    // isInternalDir of path resolution completed within the mount table 
    boolean isInternalDir() {
      return (kind == ResultKind.isInternalDir);
    }
  }
  
  /**
   * Resolve the pathname p relative to root InodeDir
   * @param p - inout path
   * @param resolveLastComponent 
   * @return ResolveResult which allows further resolution of the remaining path
   * @throws FileNotFoundException
   */
  ResolveResult<T> resolve(final String p, final boolean resolveLastComponent)
    throws FileNotFoundException {
    // TO DO: - more efficient to not split the path, but simply compare
    String[] path = breakIntoPathComponents(p); 
    if (path.length <= 1) { // special case for when path is "/"
      ResolveResult<T> res = 
        new ResolveResult<T>(ResultKind.isInternalDir, 
              root.InodeDirFs, root.fullPath, SlashPath);
      return res;
    }
    
    INodeDir<T> curInode = root;
    int i;
    // ignore first slash
    for (i = 1; i < path.length - (resolveLastComponent ? 0 : 1); i++) {
      INode<T> nextInode = curInode.resolveInternal(path[i]);
      if (nextInode == null) {
        StringBuilder failedAt = new StringBuilder(path[0]);
        for ( int j = 1; j <=i; ++j) {
          failedAt.append('/').append(path[j]);
        }
        throw (new FileNotFoundException(failedAt.toString()));      
      }

      if (nextInode instanceof INodeLink) {
        final INodeLink<T> link = (INodeLink<T>) nextInode;
        final Path remainingPath;
        if (i >= path.length-1) {
          remainingPath = SlashPath;
        } else {
          StringBuilder remainingPathStr = new StringBuilder("/" + path[i+1]);
          for (int j = i+2; j< path.length; ++j) {
            remainingPathStr.append('/').append(path[j]);
          }
          remainingPath = new Path(remainingPathStr.toString());
        }
        final ResolveResult<T> res = 
          new ResolveResult<T>(ResultKind.isExternalDir,
              link.targetFileSystem, nextInode.fullPath, remainingPath);
        return res;
      } else if (nextInode instanceof INodeDir) {
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
      for (int j = i+1; j< path.length; ++j) {
        remainingPathStr.append('/').append(path[j]);
      }
      remainingPath = new Path(remainingPathStr.toString());
    }
    final ResolveResult<T> res = 
       new ResolveResult<T>(ResultKind.isInternalDir,
           curInode.InodeDirFs, curInode.fullPath, remainingPath); 
    return res;
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
