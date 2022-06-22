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

import org.apache.hadoop.runc.squashfs.data.DataBlock;
import org.apache.hadoop.runc.squashfs.data.DataBlockCache;
import org.apache.hadoop.runc.squashfs.data.DataBlockReader;
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.directory.DirectoryHeader;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.apache.hadoop.runc.squashfs.metadata.FileMetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockCache;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataReader;
import org.apache.hadoop.runc.squashfs.metadata.TaggedMetadataBlockReader;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.table.ExportTable;
import org.apache.hadoop.runc.squashfs.table.FileTableReader;
import org.apache.hadoop.runc.squashfs.table.FragmentTable;
import org.apache.hadoop.runc.squashfs.table.IdTable;
import org.apache.hadoop.runc.squashfs.table.TableReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FileSquashFsReader extends AbstractSquashFsReader {

  private final int tag;
  private final RandomAccessFile raf;
  private final SuperBlock superBlock;
  private final MetadataBlockCache metaReader;
  private final DataBlockCache dataCache;
  private final DataBlockCache fragmentCache;
  private final IdTable idTable;
  private final FragmentTable fragmentTable;
  private final ExportTable exportTable;
  private final byte[] sparseBlock;

  FileSquashFsReader(int tag, File inputFile)
      throws SquashFsException, IOException {
    this(tag, inputFile, new MetadataBlockCache(
            new TaggedMetadataBlockReader(false)),
        DataBlockCache.NO_CACHE,
        DataBlockCache.NO_CACHE);
  }

  FileSquashFsReader(
      int tag,
      File inputFile,
      MetadataBlockCache metadataCache,
      DataBlockCache dataCache,
      DataBlockCache fragmentCache) throws SquashFsException, IOException {
    this.tag = tag;
    this.dataCache = dataCache;
    this.fragmentCache = fragmentCache;
    raf = new RandomAccessFile(inputFile, "r");
    superBlock = readSuperBlock(raf);
    sparseBlock = createSparseBlock(superBlock);

    this.metaReader = metadataCache;
    metaReader
        .add(tag, new FileMetadataBlockReader(tag, raf, superBlock, false));
    idTable = readIdTable(tag, raf, metaReader);
    fragmentTable = readFragmentTable(tag, raf, metaReader);
    exportTable = readExportTable(tag, raf, metaReader);
  }

  static SuperBlock readSuperBlock(RandomAccessFile raf)
      throws IOException, SquashFsException {
    raf.seek(0L);
    return SuperBlock.read(raf);
  }

  static IdTable readIdTable(
      int tag,
      RandomAccessFile raf,
      MetadataBlockReader metaReader) throws IOException, SquashFsException {

    TableReader tr =
        new FileTableReader(raf, metaReader.getSuperBlock(tag), false);
    return IdTable.read(tag, tr, metaReader);
  }

  static FragmentTable readFragmentTable(
      int tag,
      RandomAccessFile raf,
      MetadataBlockReader metaReader) throws IOException, SquashFsException {

    TableReader tr =
        new FileTableReader(raf, metaReader.getSuperBlock(tag), false);
    return FragmentTable.read(tag, tr, metaReader);
  }

  static ExportTable readExportTable(
      int tag,
      RandomAccessFile raf,
      MetadataBlockReader metaReader) throws IOException, SquashFsException {

    TableReader tr =
        new FileTableReader(raf, metaReader.getSuperBlock(tag), false);
    return ExportTable.read(tag, tr, metaReader);
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }

  @Override
  protected byte[] getSparseBlock() {
    return sparseBlock;
  }

  @Override
  public SuperBlock getSuperBlock() {
    return superBlock;
  }

  @Override
  public IdTable getIdTable() {
    return idTable;
  }

  @Override
  public FragmentTable getFragmentTable() {
    return fragmentTable;
  }

  @Override
  public ExportTable getExportTable() {
    return exportTable;
  }

  @Override
  public MetadataBlockReader getMetaReader() {
    return metaReader;
  }

  @Override
  public DirectoryINode getRootInode() throws IOException, SquashFsException {
    SuperBlock sb = metaReader.getSuperBlock(tag);
    long rootInodeRef = sb.getRootInodeRef();
    MetadataReader rootInodeReader = metaReader.inodeReader(tag, rootInodeRef);
    INode parent = INode.read(metaReader.getSuperBlock(tag), rootInodeReader);
    if (!(parent instanceof DirectoryINode)) {
      throw new SquashFsException(
          "Archive corrupt: root inode is not a directory");
    }
    DirectoryINode dirInode = (DirectoryINode) parent;
    return dirInode;
  }

  @Override
  public INode findInodeByInodeRef(INodeRef ref)
      throws IOException, SquashFsException {
    MetadataReader inodeReader = metaReader.inodeReader(tag, ref.getRaw());
    return INode.read(metaReader.getSuperBlock(tag), inodeReader);
  }

  @Override
  public INode findInodeByDirectoryEntry(DirectoryEntry entry)
      throws IOException, SquashFsException {
    MetadataReader inodeReader = metaReader.inodeReader(tag, entry);
    return INode.read(metaReader.getSuperBlock(tag), inodeReader);
  }

  @Override
  public INode findInodeByPath(String path)
      throws IOException, SquashFsException, FileNotFoundException {
    long rootInodeRef = superBlock.getRootInodeRef();
    MetadataReader rootInodeReader = metaReader.inodeReader(tag, rootInodeRef);
    INode parent = INode.read(metaReader.getSuperBlock(tag), rootInodeReader);

    // normalize path
    String[] parts =
        path.replaceAll("^/+", "").replaceAll("/+$", "").split("/+");

    for (String part : parts) {
      byte[] left = part.getBytes(StandardCharsets.ISO_8859_1);

      if (!(parent instanceof DirectoryINode)) {
        throw new FileNotFoundException(path);
      }
      DirectoryINode dirInode = (DirectoryINode) parent;

      MetadataReader dirReader = metaReader.directoryReader(tag, dirInode);
      int bytesToRead = dirInode.getFileSize() - 3;
      boolean found = false;
      while (dirReader.position() < bytesToRead) {
        DirectoryHeader header = DirectoryHeader.read(dirReader);
        for (int i = 0; i <= header.getCount(); i++) {
          DirectoryEntry entry = DirectoryEntry.read(header, dirReader);
          byte[] right = entry.getName();
          int compare = compareBytes(left, right);
          if (compare == 0) {
            found = true;
            parent = INode.read(superBlock, metaReader.inodeReader(tag, entry));
            break;
          } else if (compare < 0) {
            // went past
            throw new FileNotFoundException(path);
          }
        }

        if (found) {
          break;
        }
      }
      if (!found) {
        throw new FileNotFoundException(path);
      }
    }

    return parent;
  }

  @Override
  public List<DirectoryEntry> getChildren(INode parent)
      throws IOException, SquashFsException {
    if (!(parent instanceof DirectoryINode)) {
      throw new IllegalArgumentException("Inode is not a directory");
    }

    DirectoryINode dirInode = (DirectoryINode) parent;

    List<DirectoryEntry> dirEntries = new ArrayList<>();

    MetadataReader dirReader = metaReader.directoryReader(tag, dirInode);

    int dirSize = dirInode.getFileSize();
    if (dirSize > 0) {
      int bytesToRead = dirSize - 3;

      while (dirReader.position() < bytesToRead) {
        DirectoryHeader header = DirectoryHeader.read(dirReader);
        for (int i = 0; i <= header.getCount(); i++) {
          DirectoryEntry entry = DirectoryEntry.read(header, dirReader);
          dirEntries.add(entry);
        }
      }
      if (dirReader.position() != bytesToRead) {
        throw new SquashFsException(String.format("Read %d bytes, expected %d",
            dirReader.position(), bytesToRead));
      }
    }
    return dirEntries;
  }

  protected DataBlock readBlock(FileINode fileInode, int blockNumber,
      boolean cache)
      throws IOException, SquashFsException {

    return DataBlockReader
        .readBlock(tag, raf, superBlock, fileInode, blockNumber,
            cache ? dataCache : DataBlockCache.NO_CACHE);
  }

  protected DataBlock readFragment(FileINode fileInode, int fragmentSize,
      boolean cache)
      throws IOException, SquashFsException {

    return DataBlockReader.readFragment(
        tag, raf, superBlock, fileInode, fragmentTable, fragmentSize,
        cache ? fragmentCache : DataBlockCache.NO_CACHE);
  }

}
