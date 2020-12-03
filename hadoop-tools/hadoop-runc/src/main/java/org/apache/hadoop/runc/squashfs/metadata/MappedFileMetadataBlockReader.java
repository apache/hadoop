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
import org.apache.hadoop.runc.squashfs.io.ByteBufferDataInput;
import org.apache.hadoop.runc.squashfs.io.MappedFile;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.IOException;

public class MappedFileMetadataBlockReader implements MetadataBlockReader {

  private final int ourTag;
  private final SuperBlock sb;
  private final MappedFile mmap;

  public MappedFileMetadataBlockReader(int tag, SuperBlock sb, MappedFile mmap)
      throws IOException, SquashFsException {
    this.ourTag = tag;
    this.sb = sb;
    this.mmap = mmap;
  }

  @Override
  public SuperBlock getSuperBlock(int tag) {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    return sb;
  }

  @Override
  public MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }

    return MetadataBlock
        .read(new ByteBufferDataInput(mmap.from(fileOffset)), sb);
  }

  @Override
  public void close() {
    // nothing to do
  }

}
