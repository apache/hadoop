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

import org.apache.hadoop.runc.squashfs.data.DataBlockWriter;
import org.apache.hadoop.runc.squashfs.data.FragmentWriter;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.table.IdTableGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class SquashFsWriter implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(SquashFsWriter.class);

  private final RandomAccessFile raf;
  private final IdTableGenerator idGenerator;
  private final SuperBlock superBlock;
  private final SquashFsTree fsTree;
  private final DataBlockWriter dataWriter;
  private final FragmentWriter fragmentWriter;
  private final byte[] blockBuffer;

  private Integer modificationTime = null;

  public SquashFsWriter(File outputFile) throws SquashFsException, IOException {
    raf = new RandomAccessFile(outputFile, "rw");
    writeDummySuperblock(raf);
    superBlock = createSuperBlock();
    blockBuffer = createBlockBuffer(superBlock);
    idGenerator = createIdTableGenerator();
    fsTree = createSquashFsTree();
    dataWriter = createDataWriter(superBlock, raf);
    fragmentWriter = createFragmentWriter(superBlock, raf);
  }

  public void setModificationTime(int modificationTime) {
    this.modificationTime = modificationTime;
  }

  static void writeDummySuperblock(RandomAccessFile raf) throws IOException {
    raf.seek(0L);
    raf.write(new byte[SuperBlock.SIZE]);
  }

  static SuperBlock createSuperBlock() {
    return new SuperBlock();
  }

  static byte[] createBlockBuffer(SuperBlock sb) {
    return new byte[sb.getBlockSize()];
  }

  static IdTableGenerator createIdTableGenerator() {
    IdTableGenerator idGenerator = new IdTableGenerator();
    idGenerator.addUidGid(0);
    return idGenerator;
  }

  static SquashFsTree createSquashFsTree() {
    return new SquashFsTree();
  }

  static DataBlockWriter createDataWriter(SuperBlock sb, RandomAccessFile raf) {
    return new DataBlockWriter(raf, sb.getBlockSize());
  }

  static FragmentWriter createFragmentWriter(SuperBlock sb,
      RandomAccessFile raf) {
    return new FragmentWriter(raf, sb.getBlockSize());
  }

  SuperBlock getSuperBlock() {
    return superBlock;
  }

  IdTableGenerator getIdGenerator() {
    return idGenerator;
  }

  DataBlockWriter getDataWriter() {
    return dataWriter;
  }

  FragmentWriter getFragmentWriter() {
    return fragmentWriter;
  }

  byte[] getBlockBuffer() {
    return blockBuffer;
  }

  public SquashFsTree getFsTree() {
    return fsTree;
  }

  public SquashFsEntryBuilder entry(String name) {
    return new SquashFsEntryBuilder(this, name);
  }

  public void finish() throws SquashFsException, IOException {
    // flush any remaining fragments
    fragmentWriter.flush();

    // build the directory tree
    fsTree.build();

    // save inode table
    long inodeTableStart = raf.getFilePointer();
    LOG.debug("Inode table start: {}", inodeTableStart);
    fsTree.getINodeWriter().save(raf);

    // save directory table
    long dirTableStart = raf.getFilePointer();
    LOG.debug("Directory table start: {}", dirTableStart);
    fsTree.getDirWriter().save(raf);

    // build fragment table
    long fragMetaStart = raf.getFilePointer();
    MetadataWriter fragMetaWriter = new MetadataWriter();
    List<MetadataBlockRef> fragRefs = fragmentWriter.save(fragMetaWriter);
    fragMetaWriter.save(raf);

    // save fragment table
    long fragTableStart = raf.getFilePointer();
    LOG.debug("Fragment table start: {}", fragTableStart);
    for (MetadataBlockRef fragRef : fragRefs) {
      long fragTableFileOffset = fragMetaStart + fragRef.getLocation();
      byte[] buf = new byte[8];
      ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
      bb.putLong(fragTableFileOffset);
      raf.write(buf);
    }

    // build export table
    long exportMetaStart = raf.getFilePointer();
    MetadataWriter exportMetaWriter = new MetadataWriter();
    List<MetadataBlockRef> exportRefs =
        fsTree.saveExportTable(exportMetaWriter);
    exportMetaWriter.save(raf);

    // write export table
    long exportTableStart = raf.getFilePointer();
    LOG.debug("Export table start: {}", exportTableStart);
    for (MetadataBlockRef exportRef : exportRefs) {
      long exportFileOffset = exportMetaStart + exportRef.getLocation();
      byte[] buf = new byte[8];
      ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
      bb.putLong(exportFileOffset);
      raf.write(buf);
    }

    // build ID table
    long idMetaStart = raf.getFilePointer();
    MetadataWriter idMetaWriter = new MetadataWriter();
    List<MetadataBlockRef> idRefs = idGenerator.save(idMetaWriter);
    idMetaWriter.save(raf);

    MetadataBlockRef rootInodeRef = fsTree.getRootInodeRef();
    LOG.debug("Root inode ref: {}", rootInodeRef);

    // write ID table
    long idTableStart = raf.getFilePointer();
    LOG.debug("ID table start: {}", idTableStart);

    for (MetadataBlockRef idRef : idRefs) {
      long idFileOffset = idMetaStart + idRef.getLocation();
      byte[] buf = new byte[8];
      ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
      bb.putLong(idFileOffset);
      raf.write(buf);
    }

    long archiveSize = raf.getFilePointer();
    LOG.debug("Archive size:{}", archiveSize);

    // pad to 4096 bytes
    int padding = (4096 - ((int) (archiveSize % 4096L))) % 4096;
    for (int i = 0; i < padding; i++) {
      raf.write(0);
    }

    long fileSize = raf.getFilePointer();
    LOG.debug("File size: {}", fileSize);

    if (modificationTime == null) {
      modificationTime = (int) (System.currentTimeMillis() / 1000L);
    }

    // update superblock
    superBlock.setInodeCount(fsTree.getInodeCount());
    superBlock.setModificationTime(modificationTime);
    superBlock.setFragmentEntryCount(fragmentWriter.getFragmentEntryCount());
    superBlock.setIdCount((short) idGenerator.getIdCount());
    superBlock.setRootInodeRef(rootInodeRef.toINodeRefRaw());
    superBlock.setBytesUsed(archiveSize);
    superBlock.setIdTableStart(idTableStart);
    superBlock.setInodeTableStart(inodeTableStart);
    superBlock.setDirectoryTableStart(dirTableStart);
    superBlock.setFragmentTableStart(fragTableStart);
    superBlock.setExportTableStart(exportTableStart);

    LOG.debug("Superblock: {}", superBlock);

    // write superblock
    raf.seek(0L);
    superBlock.writeData(raf);
    raf.seek(fileSize);
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }

}
