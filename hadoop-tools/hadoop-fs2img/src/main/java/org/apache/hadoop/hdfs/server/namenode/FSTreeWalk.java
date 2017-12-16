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

/**
 * Traversal of an external FileSystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FSTreeWalk extends TreeWalk {

  private final Path root;
  private final FileSystem fs;

  public FSTreeWalk(Path root, Configuration conf) throws IOException {
    this.root = root;
    fs = root.getFileSystem(conf);
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
        ret.add(new TreePath(s, id, i, fs));
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
      getPendingQueue().addFirst(
          new TreePath(p.getFileStatus(), p.getParentId(), this, fs));
    }

    FSTreeIterator(Path p) throws IOException {
      try {
        FileStatus s = fs.getFileStatus(root);
        getPendingQueue().addFirst(new TreePath(s, -1L, this, fs));
      } catch (FileNotFoundException e) {
        if (p.equals(root)) {
          throw e;
        }
        throw new ConcurrentModificationException("FS modified");
      }
    }

    @Override
    public TreeIterator fork() {
      if (getPendingQueue().isEmpty()) {
        return new FSTreeIterator();
      }
      return new FSTreeIterator(getPendingQueue().removeFirst());
    }

  }

  @Override
  public TreeIterator iterator() {
    try {
      return new FSTreeIterator(root);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
