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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Random, repeatable hierarchy generator.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RandomTreeWalk extends TreeWalk {

  private final Path root;
  private final long seed;
  private final float depth;
  private final int children;
  private final Map<Long, Long> mSeed;

  RandomTreeWalk(long seed) {
    this(seed, 10);
  }

  RandomTreeWalk(long seed, int children) {
    this(seed, children, 0.15f);
  }

  RandomTreeWalk(long seed, int children, float depth) {
    this(randomRoot(seed), seed, children, depth);
  }

  RandomTreeWalk(Path root, long seed, int children, float depth) {
    this.seed = seed;
    this.depth = depth;
    this.children = children;
    mSeed = Collections.synchronizedMap(new HashMap<Long, Long>());
    mSeed.put(-1L, seed);
    this.root = root;
  }

  static Path randomRoot(long seed) {
    Random r = new Random(seed);
    String scheme;
    do {
      scheme = genName(r, 3, 5).toLowerCase();
    } while (Character.isDigit(scheme.charAt(0)));
    String authority = genName(r, 3, 15).toLowerCase();
    int port = r.nextInt(1 << 13) + 1000;
    return new Path(scheme, authority + ":" + port, "/");
  }

  @Override
  public TreeIterator iterator() {
    return new RandomTreeIterator(seed);
  }

  @Override
  protected Iterable<TreePath> getChildren(TreePath p, long id,
      TreeIterator walk) {
    final FileStatus pFs = p.getFileStatus();
    if (pFs.isFile()) {
      return Collections.emptyList();
    }
    // seed is f(parent seed, attrib)
    long cseed = mSeed.get(p.getParentId()) * p.getFileStatus().hashCode();
    mSeed.put(p.getId(), cseed);
    Random r = new Random(cseed);

    int nChildren = r.nextInt(children);
    ArrayList<TreePath> ret = new ArrayList<TreePath>();
    for (int i = 0; i < nChildren; ++i) {
      ret.add(new TreePath(genFileStatus(p, r), p.getId(), walk));
    }
    return ret;
  }

  FileStatus genFileStatus(TreePath parent, Random r) {
    final int blocksize = 128 * (1 << 20);
    final Path name;
    final boolean isDir;
    if (null == parent) {
      name = root;
      isDir = true;
    } else {
      Path p = parent.getFileStatus().getPath();
      name = new Path(p, genName(r, 3, 10));
      isDir = r.nextFloat() < depth;
    }
    final long len = isDir ? 0 : r.nextInt(Integer.MAX_VALUE);
    final int nblocks = 0 == len ? 0 : (((int)((len - 1) / blocksize)) + 1);
    BlockLocation[] blocks = genBlocks(r, nblocks, blocksize, len);
    return new LocatedFileStatus(new FileStatus(
        len,              /* long length,             */
        isDir,            /* boolean isdir,           */
        1,                /* int block_replication,   */
        blocksize,        /* long blocksize,          */
        0L,               /* long modification_time,  */
        0L,               /* long access_time,        */
        null,             /* FsPermission permission, */
        "hadoop",         /* String owner,            */
        "hadoop",         /* String group,            */
        name),            /* Path path                */
        blocks);
  }

  BlockLocation[] genBlocks(Random r, int nblocks, int blocksize, long len) {
    BlockLocation[] blocks = new BlockLocation[nblocks];
    if (0 == nblocks) {
      return blocks;
    }
    for (int i = 0; i < nblocks - 1; ++i) {
      blocks[i] = new BlockLocation(null, null, i * blocksize, blocksize);
    }
    blocks[nblocks - 1] = new BlockLocation(null, null,
        (nblocks - 1) * blocksize,
        0 == (len % blocksize) ? blocksize : len % blocksize);
    return blocks;
  }

  static String genName(Random r, int min, int max) {
    int len = r.nextInt(max - min + 1) + min;
    char[] ret = new char[len];
    while (len > 0) {
      int c = r.nextInt() & 0x7F; // restrict to ASCII
      if (Character.isLetterOrDigit(c)) {
        ret[--len] = (char) c;
      }
    }
    return new String(ret);
  }

  class RandomTreeIterator extends TreeIterator {

    RandomTreeIterator() {
    }

    RandomTreeIterator(long seed) {
      Random r = new Random(seed);
      FileStatus iroot = genFileStatus(null, r);
      getPendingQueue().addFirst(new TreePath(iroot, -1, this));
    }

    RandomTreeIterator(TreePath p) {
      getPendingQueue().addFirst(
          new TreePath(p.getFileStatus(), p.getParentId(), this));
    }

    @Override
    public TreeIterator fork() {
      if (getPendingQueue().isEmpty()) {
        return new RandomTreeIterator();
      }
      return new RandomTreeIterator(getPendingQueue().removeFirst());
    }

  }

}
