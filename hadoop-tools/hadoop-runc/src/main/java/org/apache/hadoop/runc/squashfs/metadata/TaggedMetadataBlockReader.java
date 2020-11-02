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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TaggedMetadataBlockReader implements MetadataBlockReader {

  private final boolean close;
  private final Map<Integer, MetadataBlockReader> readers = new HashMap<>();

  public TaggedMetadataBlockReader(boolean close) {
    this.close = close;
  }

  public synchronized void add(int tag, MetadataBlockReader reader) {
    Integer key = Integer.valueOf(tag);
    if (readers.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format("Tag '%d' is already in use", tag));
    }
    readers.put(key, reader);
  }

  @Override
  public synchronized void close() throws IOException {
    if (close) {
      for (MetadataBlockReader reader : readers.values()) {
        reader.close();
      }
    }
  }

  synchronized MetadataBlockReader readerFor(int tag) {
    MetadataBlockReader mbr = readers.get(Integer.valueOf(tag));
    if (mbr == null) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    return mbr;
  }

  @Override
  public MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException {
    return readerFor(tag).read(tag, fileOffset);
  }

  @Override
  public SuperBlock getSuperBlock(int tag) {
    return readerFor(tag).getSuperBlock(tag);
  }

}
