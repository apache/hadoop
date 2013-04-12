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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * A node type that can be built into a tree reflecting the
 * hierarchy of replicas on the local disk.
 */
class LDir {
  final File dir;
  final int maxBlocksPerDir;

  private int numBlocks = 0;
  private LDir[] children = null;
  private int lastChildIdx = 0;

  LDir(File dir, int maxBlocksPerDir) throws IOException {
    this.dir = dir;
    this.maxBlocksPerDir = maxBlocksPerDir;

    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + dir);
      }
    } else {
      File[] files = FileUtil.listFiles(dir); 
      List<LDir> dirList = new ArrayList<LDir>();
      for (int idx = 0; idx < files.length; idx++) {
        if (files[idx].isDirectory()) {
          dirList.add(new LDir(files[idx], maxBlocksPerDir));
        } else if (Block.isBlockFilename(files[idx])) {
          numBlocks++;
        }
      }
      if (dirList.size() > 0) {
        children = dirList.toArray(new LDir[dirList.size()]);
      }
    }
  }
      
  File addBlock(Block b, File src) throws IOException {
    //First try without creating subdirectories
    File file = addBlock(b, src, false, false);          
    return (file != null) ? file : addBlock(b, src, true, true);
  }

  private File addBlock(Block b, File src, boolean createOk, boolean resetIdx
      ) throws IOException {
    if (numBlocks < maxBlocksPerDir) {
      final File dest = FsDatasetImpl.moveBlockFiles(b, src, dir);
      numBlocks += 1;
      return dest;
    }
          
    if (lastChildIdx < 0 && resetIdx) {
      //reset so that all children will be checked
      lastChildIdx = DFSUtil.getRandom().nextInt(children.length);              
    }
          
    if (lastChildIdx >= 0 && children != null) {
      //Check if any child-tree has room for a block.
      for (int i=0; i < children.length; i++) {
        int idx = (lastChildIdx + i)%children.length;
        File file = children[idx].addBlock(b, src, false, resetIdx);
        if (file != null) {
          lastChildIdx = idx;
          return file; 
        }
      }
      lastChildIdx = -1;
    }
          
    if (!createOk) {
      return null;
    }
          
    if (children == null || children.length == 0) {
      children = new LDir[maxBlocksPerDir];
      for (int idx = 0; idx < maxBlocksPerDir; idx++) {
        final File sub = new File(dir, DataStorage.BLOCK_SUBDIR_PREFIX+idx);
        children[idx] = new LDir(sub, maxBlocksPerDir);
      }
    }
          
    //now pick a child randomly for creating a new set of subdirs.
    lastChildIdx = DFSUtil.getRandom().nextInt(children.length);
    return children[ lastChildIdx ].addBlock(b, src, true, false); 
  }

  void getVolumeMap(String bpid, ReplicaMap volumeMap, FsVolumeImpl volume
      ) throws IOException {
    if (children != null) {
      for (int i = 0; i < children.length; i++) {
        children[i].getVolumeMap(bpid, volumeMap, volume);
      }
    }

    recoverTempUnlinkedBlock();
    volume.addToReplicasMap(bpid, volumeMap, dir, true);
  }
      
  /**
   * Recover unlinked tmp files on datanode restart. If the original block
   * does not exist, then the tmp file is renamed to be the
   * original file name; otherwise the tmp file is deleted.
   */
  private void recoverTempUnlinkedBlock() throws IOException {
    File files[] = FileUtil.listFiles(dir);
    for (File file : files) {
      if (!FsDatasetUtil.isUnlinkTmpFile(file)) {
        continue;
      }
      File blockFile = FsDatasetUtil.getOrigFile(file);
      if (blockFile.exists()) {
        // If the original block file still exists, then no recovery  is needed.
        if (!file.delete()) {
          throw new IOException("Unable to cleanup unlinked tmp file " + file);
        }
      } else {
        if (!file.renameTo(blockFile)) {
          throw new IOException("Unable to cleanup detached file " + file);
        }
      }
    }
  }
  
  /**
   * check if a data diretory is healthy
   * @throws DiskErrorException
   */
  void checkDirTree() throws DiskErrorException {
    DiskChecker.checkDir(dir);
          
    if (children != null) {
      for (int i = 0; i < children.length; i++) {
        children[i].checkDirTree();
      }
    }
  }
      
  void clearPath(File f) {
    String root = dir.getAbsolutePath();
    String dir = f.getAbsolutePath();
    if (dir.startsWith(root)) {
      String[] dirNames = dir.substring(root.length()).
        split(File.separator + DataStorage.BLOCK_SUBDIR_PREFIX);
      if (clearPath(f, dirNames, 1))
        return;
    }
    clearPath(f, null, -1);
  }
      
  /**
   * dirNames is an array of string integers derived from
   * usual directory structure data/subdirN/subdirXY/subdirM ...
   * If dirName array is non-null, we only check the child at 
   * the children[dirNames[idx]]. This avoids iterating over
   * children in common case. If directory structure changes 
   * in later versions, we need to revisit this.
   */
  private boolean clearPath(File f, String[] dirNames, int idx) {
    if ((dirNames == null || idx == dirNames.length) &&
        dir.compareTo(f) == 0) {
      numBlocks--;
      return true;
    }
        
    if (dirNames != null) {
      //guess the child index from the directory name
      if (idx > (dirNames.length - 1) || children == null) {
        return false;
      }
      int childIdx; 
      try {
        childIdx = Integer.parseInt(dirNames[idx]);
      } catch (NumberFormatException ignored) {
        // layout changed? we could print a warning.
        return false;
      }
      return (childIdx >= 0 && childIdx < children.length) ?
        children[childIdx].clearPath(f, dirNames, idx+1) : false;
    }

    //guesses failed. back to blind iteration.
    if (children != null) {
      for(int i=0; i < children.length; i++) {
        if (children[i].clearPath(f, null, -1)){
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "FSDir{dir=" + dir + ", children="
        + (children == null ? null : Arrays.asList(children)) + "}";
  }
}