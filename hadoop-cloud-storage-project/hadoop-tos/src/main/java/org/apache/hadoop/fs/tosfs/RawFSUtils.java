/*
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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Preconditions;

import java.util.Objects;

public final class RawFSUtils {
  private RawFSUtils() {
  }

  /**
   * Check whether root is the parent of p.
   *
   * @param root the root path.
   * @param p    the path to check.
   * @return true means the node is included in the subtree which has the root node.
   */
  public static boolean inSubtree(String root, String p) {
    return inSubtree(new Path(root), new Path(p));
  }

  /**
   * Check whether root is the parent of node.
   *
   * @param root the root path.
   * @param node the path to check.
   * @return true means the node is included in the subtree which has the root node.
   */
  public static boolean inSubtree(Path root, Path node) {
    Preconditions.checkNotNull(root, "Root cannot be null");
    Preconditions.checkNotNull(node, "Node cannot be null");
    if (root.isRoot()) {
      return true;
    }

    if (Objects.equals(root, node)) {
      return true;
    }

    while (!node.isRoot()) {
      if (Objects.equals(root, node)) {
        return true;
      }
      node = node.getParent();
    }
    return false;
  }
}
