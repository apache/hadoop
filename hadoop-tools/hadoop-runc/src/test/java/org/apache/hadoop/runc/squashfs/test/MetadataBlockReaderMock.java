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

package org.apache.hadoop.runc.squashfs.test;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlock;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class MetadataBlockReaderMock implements MetadataBlockReader {

  private final int ourTag;
  private final SuperBlock sb;
  private final Map<Long, MetadataBlock> blockMap;
  private volatile boolean closed = false;

  public MetadataBlockReaderMock(int tag, SuperBlock sb,
      long expectedFileOffset, MetadataBlock block) {
    this(tag, sb,
        Collections.singletonMap(Long.valueOf(expectedFileOffset), block));
  }

  public MetadataBlockReaderMock(int tag, SuperBlock sb,
      Map<Long, MetadataBlock> blockMap) {
    this.ourTag = tag;
    this.sb = sb;
    this.blockMap = blockMap;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

  @Override
  public MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    MetadataBlock block = blockMap.get(Long.valueOf(fileOffset));
    if (block == null) {
      throw new EOFException(String.format("unexpected block %d", fileOffset));
    }
    return block;
  }

  @Override
  public SuperBlock getSuperBlock(int tag) {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    return sb;
  }

}
