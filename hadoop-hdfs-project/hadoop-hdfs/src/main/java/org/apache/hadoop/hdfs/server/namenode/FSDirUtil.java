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

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FSDirUtil {
  /**
   * Collect blocks under INode recursively.
   * @param iNode INode that need to collect blocks
   * @return blocks collected under INode
   */
  public static List<BlockInfo> collectBlocks(INode iNode) {
    List<BlockInfo> blocks = new ArrayList<>();
    if (iNode == null) {
      return blocks;
    }
    if (iNode.isFile()) {
      BlockInfo[] blk = iNode.asFile().getBlocks(Snapshot.CURRENT_STATE_ID);
      blocks.addAll(Arrays.asList(blk));
    } else if (iNode.isDirectory()) {
      INodeDirectory dir = iNode.asDirectory();
      for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
        List<BlockInfo> blockInfos = collectBlocks(child);
        blocks.addAll(blockInfos);
      }
    }
    return blocks;
  }

  public static List<INodeFile> collectInodeFiles(INode iNode) {
    List<INodeFile> iNodeFiles = new LinkedList<>();
    if (iNode.isDirectory()) {
      INodeDirectory dir = iNode.asDirectory();
      for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
        iNodeFiles.addAll(collectInodeFiles(child));
      }
    } else if (iNode.isFile()) {
      iNodeFiles.add(iNode.asFile());
    }
    return iNodeFiles;
  }

  public static List<INodeFile> getInodeFiles(FSNamesystem fsn, String path)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    INode iNode = fsd.getINode(path);
    return collectInodeFiles(iNode);
  }

  public static List<BlockInfo> getBlockInfos(FSNamesystem fsn,
      String path) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    INode iNode = fsd.getINode(path);
    return FSDirUtil.collectBlocks(iNode);
  }

  public static long getBlkCollectId(FSNamesystem fsn, String src)
      throws IOException {
    INode iNode =
        fsn.getFSDirectory().getINode(src, FSDirectory.DirOp.READ);
    return iNode.getId();
  }
}
