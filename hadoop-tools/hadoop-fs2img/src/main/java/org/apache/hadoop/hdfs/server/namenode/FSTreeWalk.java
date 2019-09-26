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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ACLS_IMPORT_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ACLS_IMPORT_ENABLED_DEFAULT;

/**
 * Traversal of an external FileSystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FSTreeWalk extends TreeWalk {

  public static final Logger LOG =
      LoggerFactory.getLogger(FSTreeWalk.class);

  private final Path root;
  private final FileSystem fs;
  private final boolean enableACLs;

  public FSTreeWalk(Path root, Configuration conf) throws IOException {
    this.root = root;
    fs = root.getFileSystem(conf);

    boolean mountACLsEnabled = conf.getBoolean(DFS_PROVIDED_ACLS_IMPORT_ENABLED,
        DFS_PROVIDED_ACLS_IMPORT_ENABLED_DEFAULT);
    boolean localACLsEnabled = conf.getBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    if (!localACLsEnabled && mountACLsEnabled) {
      LOG.warn("Mount ACLs have been enabled but HDFS ACLs are not. " +
          "Disabling ACLs on the mount {}", root);
      this.enableACLs = false;
    } else {
      this.enableACLs = mountACLsEnabled;
    }
  }

  @Override
  protected Iterable<TreePath> getChildren(TreePath path, long id,
      TreeIterator i) {
    // TODO symlinks
    if (!path.getFileStatus().isDirectory()) {
      return Collections.emptyList();
    }
    try {
      ArrayList<TreePath> ret = new ArrayList<>();
      for (FileStatus s : fs.listStatus(path.getFileStatus().getPath())) {
        AclStatus aclStatus = getAclStatus(fs, s.getPath());
        ret.add(new TreePath(s, id, i, fs, aclStatus));
      }
      return ret;
    } catch (FileNotFoundException e) {
      throw new ConcurrentModificationException("FS modified");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  class FSTreeIterator extends TreeIterator {

    private FSTreeIterator() {
    }

    FSTreeIterator(TreePath p) {
      this(p.getFileStatus(), p.getParentId());
    }

    FSTreeIterator(FileStatus fileStatus, long parentId) {
      Path path = fileStatus.getPath();
      AclStatus acls;
      try {
        acls = getAclStatus(fs, path);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      TreePath treePath = new TreePath(fileStatus, parentId, this, fs, acls);
      getPendingQueue().addFirst(treePath);
    }

    @Override
    public TreeIterator fork() {
      if (getPendingQueue().isEmpty()) {
        return new FSTreeIterator();
      }
      return new FSTreeIterator(getPendingQueue().removeFirst());
    }

  }

  private AclStatus getAclStatus(FileSystem fileSystem, Path path)
      throws IOException {
    return enableACLs ? fileSystem.getAclStatus(path) : null;
  }

  @Override
  public TreeIterator iterator() {
    try {
      FileStatus s = fs.getFileStatus(root);
      return new FSTreeIterator(s, -1L);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
