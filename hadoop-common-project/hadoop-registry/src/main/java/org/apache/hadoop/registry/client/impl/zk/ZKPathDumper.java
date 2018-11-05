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

package org.apache.hadoop.registry.client.impl.zk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * This class dumps a registry tree to a string.
 * It does this in the <code>toString()</code> method, so it
 * can be used in a log statement -the operation
 * will only take place if the method is evaluated.
 *
 */
@VisibleForTesting
public class ZKPathDumper {

  public static final int INDENT = 2;
  private final CuratorFramework curator;
  private final String root;
  private final boolean verbose;

  /**
   * Create a path dumper -but do not dump the path until asked
   * @param curator curator instance
   * @param root root
   * @param verbose verbose flag - includes more details (such as ACLs)
   */
  public ZKPathDumper(CuratorFramework curator,
      String root,
      boolean verbose) {
    Preconditions.checkArgument(curator != null);
    Preconditions.checkArgument(root != null);
    this.curator = curator;
    this.root = root;
    this.verbose = verbose;
  }

  /**
   * Trigger the recursive registry dump.
   * @return a string view of the registry
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ZK tree for ").append(root).append('\n');
    expand(builder, root, 1);
    return builder.toString();
  }

  /**
   * Recursively expand the path into the supplied string builder, increasing
   * the indentation by {@link #INDENT} as it proceeds (depth first) down
   * the tree
   * @param builder string build to append to
   * @param path path to examine
   * @param indent current indentation
   */
  private void expand(StringBuilder builder,
      String path,
      int indent) {
    try {
      GetChildrenBuilder childrenBuilder = curator.getChildren();
      List<String> children = childrenBuilder.forPath(path);
      for (String child : children) {
        String childPath = path + "/" + child;
        String body;
        Stat stat = curator.checkExists().forPath(childPath);
        StringBuilder bodyBuilder = new StringBuilder(256);
        bodyBuilder.append("  [")
                          .append(stat.getDataLength())
                          .append("]");
        if (stat.getEphemeralOwner() > 0) {
          bodyBuilder.append("*");
        }
        if (verbose) {
          // verbose: extract ACLs
          builder.append(" -- ");
          List<ACL> acls =
              curator.getACL().forPath(childPath);
          for (ACL acl : acls) {
            builder.append(RegistrySecurity.aclToString(acl));
            builder.append(" ");
          }
        }
        body = bodyBuilder.toString();
        // print each child
        append(builder, indent, ' ');
        builder.append('/').append(child);
        builder.append(body);
        builder.append('\n');
        // recurse
        expand(builder, childPath, indent + INDENT);
      }
    } catch (Exception e) {
      builder.append(e.toString()).append("\n");
    }
  }

  /**
   * Append the specified indentation to a builder
   * @param builder string build to append to
   * @param indent current indentation
   * @param c charactor to use for indentation
   */
  private void append(StringBuilder builder, int indent, char c) {
    for (int i = 0; i < indent; i++) {
      builder.append(c);
    }
  }
}
