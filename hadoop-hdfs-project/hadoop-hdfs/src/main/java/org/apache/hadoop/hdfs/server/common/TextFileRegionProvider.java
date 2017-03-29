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

package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class is used to read file regions from block maps
 * specified using delimited text.
 */
public class TextFileRegionProvider
    extends FileRegionProvider implements Configurable {

  private Configuration conf;
  private BlockFormat<FileRegion> fmt;

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    fmt = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.DFS_PROVIDER_BLK_FORMAT_CLASS,
            TextFileRegionFormat.class,
            BlockFormat.class),
        conf);
    ((Configurable)fmt).setConf(conf); //redundant?
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Iterator<FileRegion> iterator() {
    try {
      final BlockFormat.Reader<FileRegion> r = fmt.getReader(null);
      return new Iterator<FileRegion>() {

        private final Iterator<FileRegion> inner = r.iterator();

        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }

        @Override
        public FileRegion next() {
          return inner.next();
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

  @Override
  public void refresh() throws IOException {
    fmt.refresh();
  }
}
