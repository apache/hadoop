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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.BlockAlias;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads provided blocks from a {@link BlockFormat}.
 */
public class BlockFormatProvider extends BlockProvider
    implements Configurable {

  private Configuration conf;
  private BlockFormat<? extends BlockAlias> blockFormat;
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockFormatProvider.class);

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void setConf(Configuration conf) {
    Class<? extends BlockFormat> c = conf.getClass(
        DFSConfigKeys.DFS_PROVIDER_BLK_FORMAT_CLASS,
        TextFileRegionFormat.class, BlockFormat.class);
    blockFormat = ReflectionUtils.newInstance(c, conf);
    LOG.info("Loaded BlockFormat class : " + c.getClass().getName());
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Iterator<Block> iterator() {
    try {
      final BlockFormat.Reader<? extends BlockAlias> reader =
          blockFormat.getReader(null);

      return new Iterator<Block>() {

        private final Iterator<? extends BlockAlias> inner = reader.iterator();

        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }

        @Override
        public Block next() {
          return inner.next().getBlock();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (IOException e) {
      throw new RuntimeException("Failed to read provided blocks", e);
    }
  }

}
