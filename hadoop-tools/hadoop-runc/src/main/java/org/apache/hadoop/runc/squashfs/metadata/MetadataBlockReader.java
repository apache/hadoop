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
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.Closeable;
import java.io.IOException;

public interface MetadataBlockReader extends Closeable {

  MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException;

  SuperBlock getSuperBlock(int tag);

  default MetadataReader reader(MetadataReference metaRef)
      throws IOException, SquashFsException {
    return MetadataBlock.reader(this, metaRef);
  }

  default MetadataReader rawReader(
      int tag,
      long blockLocation,
      short offset) throws IOException, SquashFsException {
    return reader(MetadataReference.raw(tag, blockLocation, offset));
  }

  default MetadataReader inodeReader(int tag, long inodeRef)
      throws IOException, SquashFsException {
    return reader(MetadataReference.inode(tag, getSuperBlock(tag), inodeRef));
  }

  default MetadataReader inodeReader(int tag, DirectoryEntry dirEnt)
      throws IOException, SquashFsException {
    return reader(MetadataReference.inode(tag, getSuperBlock(tag), dirEnt));
  }

  default MetadataReader directoryReader(int tag, DirectoryINode dir)
      throws IOException, SquashFsException {
    return reader(MetadataReference.directory(tag, getSuperBlock(tag), dir));
  }

}
