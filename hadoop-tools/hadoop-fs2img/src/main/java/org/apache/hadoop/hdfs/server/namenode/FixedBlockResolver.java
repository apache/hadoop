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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

/**
 * Resolver mapping all files to a configurable, uniform blocksize.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FixedBlockResolver extends BlockResolver implements Configurable {

  public static final String BLOCKSIZE =
      "hdfs.image.writer.resolver.fixed.block.size";
  public static final String START_BLOCK =
      "hdfs.image.writer.resolver.fixed.block.start";
  public static final long BLOCKSIZE_DEFAULT = 256 * (1L << 20);

  private Configuration conf;
  private long blocksize = 256 * (1L << 20);
  private final AtomicLong blockIds = new AtomicLong(0);

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    blocksize = conf.getLong(BLOCKSIZE, BLOCKSIZE_DEFAULT);
    blockIds.set(conf.getLong(START_BLOCK, (1L << 30)));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  protected List<Long> blockLengths(FileStatus s) {
    ArrayList<Long> ret = new ArrayList<>();
    if (!s.isFile()) {
      return ret;
    }
    if (0 == s.getLen()) {
      // the file has length 0; so we will have one block of size 0
      ret.add(0L);
      return ret;
    }
    int nblocks = (int)((s.getLen() - 1) / blocksize) + 1;
    for (int i = 0; i < nblocks - 1; ++i) {
      ret.add(blocksize);
    }
    long rem = s.getLen() % blocksize;
    ret.add(0 == (rem % blocksize) ? blocksize : rem);
    return ret;
  }

  @Override
  public long nextId() {
    return blockIds.incrementAndGet();
  }

  @Override
  public long lastId() {
    return blockIds.get();
  }

  @Override
  public long preferredBlockSize(FileStatus s) {
    return blocksize;
  }

  @Override
  public int getReplication(FileStatus s) {
    return 1;
  }
}
