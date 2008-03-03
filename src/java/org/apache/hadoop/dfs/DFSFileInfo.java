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
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

/******************************************************
 * DFSFileInfo tracks info about remote files, including
 * name, size, etc.
 * 
 * Includes partial information about its blocks.
 * Block locations are sorted by the distance to the current client.
 * 
 ******************************************************/
class DFSFileInfo extends FileStatus {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DFSFileInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DFSFileInfo(); }
       });
  }

  /**
   */
  public DFSFileInfo() {
  }

  /**
   * Create DFSFileInfo by file INode 
   */
  public DFSFileInfo(String path, INode node) {
    // length is zero for directories
    super(node.isDirectory() ? 0 : node.computeContentSummary().getLength(), 
          node.isDirectory(), 
          node.isDirectory() ? 0 : ((INodeFile)node).getReplication(), 
          node.isDirectory() ? 0 : ((INodeFile)node).getPreferredBlockSize(),
          node.getModificationTime(),
          node.getFsPermission(),
          node.getUserName(),
          node.getGroupName(),
          new Path(path));
  }

  /**
   */
  public String getName() {
    return getPath().getName();
  }
  
  /**
   */
  public String getParent() {
    return getPath().getParent().toString();
  }

  /**
   * @deprecated use {@link #getLen()} instead
   */
  public long getContentsLen() {
    assert isDir() : "Must be a directory";
    return getLen();
  }
}
