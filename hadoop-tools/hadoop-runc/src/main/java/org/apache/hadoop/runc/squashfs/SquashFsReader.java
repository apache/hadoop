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

package org.apache.hadoop.runc.squashfs;

import org.apache.hadoop.runc.squashfs.data.DataBlockCache;
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.apache.hadoop.runc.squashfs.io.MappedFile;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockCache;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.table.ExportTable;
import org.apache.hadoop.runc.squashfs.table.FragmentTable;
import org.apache.hadoop.runc.squashfs.table.IdTable;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface SquashFsReader extends Closeable {

  static SquashFsReader fromFile(File inputFile)
      throws SquashFsException, IOException {
    return new FileSquashFsReader(0, inputFile);
  }

  static SquashFsReader fromFile(int tag, File inputFile)
      throws SquashFsException, IOException {
    return new FileSquashFsReader(tag, inputFile);
  }

  static SquashFsReader fromFile(
      int tag, File inputFile,
      MetadataBlockCache metadataCache,
      DataBlockCache dataCache,
      DataBlockCache fragmentCache) throws SquashFsException, IOException {

    return new FileSquashFsReader(tag, inputFile, metadataCache, dataCache,
        fragmentCache);
  }

  static SquashFsReader fromMappedFile(MappedFile mmap)
      throws SquashFsException, IOException {
    return new MappedSquashFsReader(0, mmap);
  }

  static SquashFsReader fromMappedFile(int tag, MappedFile mmap)
      throws SquashFsException, IOException {
    return new MappedSquashFsReader(tag, mmap);
  }

  static SquashFsReader fromMappedFile(int tag, MappedFile mmap,
      MetadataBlockCache metadataCache,
      DataBlockCache dataCache,
      DataBlockCache fragmentCache) throws SquashFsException, IOException {
    return new MappedSquashFsReader(tag, mmap, metadataCache, dataCache,
        fragmentCache);
  }

  SuperBlock getSuperBlock();

  IdTable getIdTable();

  FragmentTable getFragmentTable();

  ExportTable getExportTable();

  MetadataBlockReader getMetaReader();

  DirectoryINode getRootInode() throws IOException, SquashFsException;

  INode findInodeByInodeRef(INodeRef ref)
      throws IOException, SquashFsException;

  INode findInodeByDirectoryEntry(DirectoryEntry entry)
      throws IOException, SquashFsException;

  INode findInodeByPath(String path)
      throws IOException, SquashFsException, FileNotFoundException;

  List<DirectoryEntry> getChildren(INode parent)
      throws IOException, SquashFsException;

  long writeFileStream(INode inode, OutputStream out)
      throws IOException, SquashFsException;

  long writeFileOut(INode inode, DataOutput out)
      throws IOException, SquashFsException;

  int read(INode inode, long fileOffset, byte[] buf, int off, int len)
      throws IOException, SquashFsException;

}
