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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a {@link TreeWalk} for mounting multiple paths.
 */
public class FSMultiRootTreeWalk extends TreeWalk {

  /**
   * Class used to designate a node in the namespace tree resulting
   * from the HDFS mount paths.
   */
  public static class Node {
    private Path path;
    private Set<Node> children;
    private TreeWalk remoteWalk;

    public Node(Path path) {
      this.path = path;
      children = new HashSet<>();
    }

    public Path getPath() {
      return path;
    }

    public void addChild(Node child) {
      if (remoteWalk != null) {
        // adding children to a node that is expected to mount a remote path.
        throw new IllegalArgumentException("Path " + getPath() +
            " is expected to mount multiple paths;" +
            "merge mounts are not supported by this tool!");
      }
      children.add(child);
    }

    public Set<Node> getChildren() {
      return children;
    }

    public void setRemoteWalk(TreeWalk remoteWalk) {
      this.remoteWalk = remoteWalk;
    }

    public TreeWalk getRemoteWalk() {
      return remoteWalk;
    }
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(FSMultiRootTreeWalk.class);

  private Path[] localMounts;
  private TreeWalk[] remoteWalks;
  private Node root;
  // map of the local (HDFS) paths to the Node used to track it.
  private Map<Path, Node> hdfsNodes;
  // map of the local (HDFS) paths to the remote treewalk associated with it.
  // only the mount paths should have entries in this map.
  private Map<Path, TreeWalk> remoteTreeWalks;
  private long accessTime;

  private final FsPermission defaultPermissions =
      FsPermission.valueOf("dr-xr-xr-x");
  private final String defaultUser = "";
  private final String defaultGroup = "";

  /**
   * Constructor.
   * The i^{th} element in {@code localMounts} should correspond to the i^{th}
   * element in the {@code remoteWalks}.
   * @param localMounts list of local mount paths.
   * @param remoteWalks list of remote treewalks that are to be mounted.
   */
  public FSMultiRootTreeWalk(Path[] localMounts, TreeWalk[] remoteWalks)
      throws IOException {
    this.localMounts = localMounts;
    this.remoteWalks = remoteWalks;
    this.accessTime = Time.now();
    remoteTreeWalks = new HashMap<>();
    for (int i = 0; i < localMounts.length; i++) {
      remoteTreeWalks.put(localMounts[i], remoteWalks[i]);
    }
    createHDFSTree();
  }

  private void createHDFSTree() throws IOException {
    // create the tree required for creating the local mount points.
    root = new Node(new Path("/"));
    hdfsNodes = new HashMap<>();
    hdfsNodes.put(root.getPath(), root);
    for (int i = 0; i < localMounts.length; i++) {
      explorePath(root, localMounts[i], remoteWalks[i], hdfsNodes);
    }
  }

  /**
   * Explore the given path and add the {@link TreePath}s corresponding
   * to each path component to the {@code pathsExplored} map.
   * The node corresponding to the first component of the path is added as child
   * to {@code root}, and the remote path is added to the node corresponding to
   * the last component of the path.
   * @param root the root node to use.
   * @param hdfsMountPath the hdfs mount path.
   * @param remoteWalk the remote path that is to be mounted.
   * @param hdfsPaths the paths whose nodes have been created.
   */
  public static void explorePath(Node root, Path hdfsMountPath,
      TreeWalk remoteWalk, Map<Path, Node> hdfsPaths) throws IOException {
    // add the hdfsMountPath components to a stack
    // so that they can be checked top-down.
    Stack<String> stack = new Stack<>();
    while (hdfsMountPath != null) {
      String component = hdfsMountPath.getName();
      if (component.length() > 0) {
        stack.push(component);
      }
      hdfsMountPath = hdfsMountPath.getParent();
    }
    // the root of the hdfsMountPath is now at the top of the stack.
    // explore the hdfsMountPath down from the root.
    Path currPath = root.getPath();
    Node parentNode = root;
    Node currNode = root;

    while (!stack.isEmpty()) {
      String childDir = stack.pop();
      currPath = new Path(currPath, childDir);
      // use the node corresponding to currPath if already generated.
      if (hdfsPaths.containsKey(currPath)) {
        currNode = hdfsPaths.get(currPath);
      } else {
        currNode = new Node(currPath);
        hdfsPaths.put(currPath, currNode);
      }
      // subsequent nodes are added to the previous node
      parentNode.addChild(currNode);
      parentNode = currNode;
    }

    if (currNode.getRemoteWalk() != null || currNode.getChildren().size() > 0) {
      // have nested mounted here - either the same node was used to mount
      // multiple remote locations or node that is expected to be a leaf node
      // (as a remote path is mounted) has children.
      throw new IllegalArgumentException("Path " + currNode.getPath() +
          " is expected to mount multiple paths;" +
          "merge mounts are not supported by this tool!");
    }
    // add the remote hdfsMountPath to the last node in the path.
    currNode.setRemoteWalk(remoteWalk);
  }

  @Override
  protected Iterable<TreePath> getChildren(TreePath treePath,
      long id, TreeIterator iterator) {
    // TODO symlinks
    if (!treePath.getFileStatus().isDirectory()) {
      return Collections.emptyList();
    }
    LOG.debug("Exploring children for " + treePath.getFileStatus().getPath());
    Path currPath = treePath.getFileStatus().getPath();
    Node currNode = hdfsNodes.get(currPath);

    if (currNode != null && currNode.getChildren().size() > 0) {
      // in the virtual HDFS tree; use the Node to get the children
      Set<Node> children = currNode.getChildren();
      ArrayList<TreePath> ret = new ArrayList<>();
      for (Node child: children) {
        FileStatus status;
        AclStatus aclStatus = null;
        String storagePolicyName = null;
        if (child.getRemoteWalk() != null) {
          // root of remote walk
          TreePath remoteTreePath = getRemoteTreePath(child.getRemoteWalk());
          status = remoteTreePath.getFileStatus();
          // set the path of this FileStatus to the local path
          status = new FileStatus(0, true, -1, -1, status.getModificationTime(),
              status.getAccessTime(), status.getPermission(), status.getOwner(),
              status.getGroup(), null, child.getPath());
          aclStatus = remoteTreePath.getAclStatus();
          // set storage policy to PROVIDED only for the root of the mount.
          storagePolicyName = HdfsConstants.PROVIDED_STORAGE_POLICY_NAME;
        } else {
          // child exists in HDFS only
          status = new FileStatus(0, true, -1, -1, accessTime,
              accessTime, defaultPermissions, defaultUser, defaultGroup,
              null, child.getPath());
        }
        ret.add(new TreePath(status, id, iterator,
            null, treePath.getLocalMountPath(),
            treePath.getRemoteRoot(), aclStatus, storagePolicyName));
      }
      return ret;
    } else {
      // use the remote tree walks to get the children.
      TreeWalk remoteTreeWalk;
      Path localMount;
      if (currNode != null) {
        remoteTreeWalk = currNode.getRemoteWalk();
        localMount = currNode.getPath();
        TreeIterator remoteIterator = remoteTreeWalk.iterator();
        if (remoteIterator.hasNext()) {
          treePath = remoteIterator.next();
        } else {
          return Collections.emptyList();
        }
      } else {
        // call the remote tree walk
        localMount = treePath.getLocalMountPath();
        remoteTreeWalk = remoteTreeWalks.get(localMount);
      }
      return getChildrenFromRemoteWalk(remoteTreeWalk, localMount,
          treePath, id, iterator);
    }
  }

  private Iterable<TreePath> getChildrenFromRemoteWalk(TreeWalk remoteWalk,
      Path localMount, TreePath treePath, long id, TreeIterator iterator) {
    Iterable<TreePath> remoteTreePaths = remoteWalk.getChildren(
        treePath, id, iterator);
    ArrayList<TreePath> ret = new ArrayList<>();
    for (TreePath path: remoteTreePaths) {
      ret.add(new TreePath(path.getFileStatus(), path.getParentId(),
          path.getIterator(), null, localMount, remoteWalk.getRoot(),
          path.getAclStatus()));
    }
    return ret;
  }

  private TreePath getRemoteTreePath(TreeWalk treeWalk) {
    TreeIterator remoteIterator = treeWalk.iterator();
    if (remoteIterator.hasNext()) {
      return remoteIterator.next();
    }
    throw new IllegalStateException("Remote tree walk "
        + treeWalk + " did not yield any paths");
  }

  class MultiRootTreeIterator extends TreeIterator {

    MultiRootTreeIterator(Node root) {
      FileStatus stat;
      AclStatus aclStatus = null;
      TreeWalk remoteTreeWalk = remoteTreeWalks.get(root.getPath());
      if (remoteTreeWalk != null) {
        // root is the mount point.
        TreePath remoteTreePath = getRemoteTreePath(remoteTreeWalk);
        stat = remoteTreePath.getFileStatus();
        aclStatus = remoteTreePath.getAclStatus();
      } else {
        stat = new FileStatus(0, true, -1, -1, accessTime,
            accessTime, defaultPermissions, defaultUser, defaultGroup,
            null, root.getPath());
      }
      TreePath rootPath = new TreePath(stat, -1, this, null, root.getPath(),
          root.getPath(), aclStatus);
      getPendingQueue().add(rootPath);
    }

    @Override
    public TreeIterator fork() {
      throw new UnsupportedOperationException(
          "Fork not implemented for MultiRootTreeIterator");
    }
  }

  @Override
  public TreeIterator iterator() {
    return new MultiRootTreeIterator(root);
  }

  @Override
  public Path getRoot() {
    return root.getPath();
  }
}
