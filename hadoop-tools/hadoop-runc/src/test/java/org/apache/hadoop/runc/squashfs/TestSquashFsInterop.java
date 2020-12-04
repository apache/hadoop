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
import org.apache.hadoop.runc.squashfs.inode.DeviceINode;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.inode.SymlinkINode;
import org.apache.hadoop.runc.squashfs.io.MappedFile;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockCache;
import org.apache.hadoop.runc.squashfs.metadata.TaggedMetadataBlockReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(Parameterized.class)
public class TestSquashFsInterop {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"file", (ReaderCreator) (a -> createFileReader(a))},
        {"file-with-cache",
            (ReaderCreator) (a -> createFileReaderWithCache(a))},
        {"mapped", (ReaderCreator) (a -> createMappedReader(a))},
        {"mapped-with-cache",
            (ReaderCreator) (a -> createMappedReaderWithCache(a))}});
  }

  @FunctionalInterface
  public interface ReaderCreator {
    SquashFsReader create(File archive)
        throws SquashFsException, IOException;
  }

  private ReaderCreator creator;

  public TestSquashFsInterop(String testName, ReaderCreator creator) {
    this.creator = creator;
  }

  public SquashFsReader createReader(File archive)
      throws SquashFsException, IOException {
    return creator.create(archive);
  }

  public static SquashFsReader createFileReader(File archive)
      throws SquashFsException, IOException {
    return SquashFsReader.fromFile(archive);
  }

  public static SquashFsReader createFileReaderWithCache(File archive)
      throws SquashFsException, IOException {
    MetadataBlockCache cache =
        new MetadataBlockCache(new TaggedMetadataBlockReader(true));
    return SquashFsReader.fromFile(0, archive, cache, new DataBlockCache(64),
        new DataBlockCache(64));
  }

  public static SquashFsReader createMappedReader(File archive)
      throws SquashFsException, IOException {
    MappedFile mmap;
    try (RandomAccessFile raf = new RandomAccessFile(archive, "r")) {
      mmap = MappedFile.mmap(raf.getChannel(),
          MappedSquashFsReader.PREFERRED_MAP_SIZE,
          MappedSquashFsReader.PREFERRED_WINDOW_SIZE);
    }
    return SquashFsReader.fromMappedFile(0, mmap);
  }

  public static SquashFsReader createMappedReaderWithCache(File archive)
      throws SquashFsException, IOException {
    MetadataBlockCache cache =
        new MetadataBlockCache(new TaggedMetadataBlockReader(false));
    MappedFile mmap;
    try (RandomAccessFile raf = new RandomAccessFile(archive, "r")) {
      mmap = MappedFile.mmap(raf.getChannel(),
          MappedSquashFsReader.PREFERRED_MAP_SIZE,
          MappedSquashFsReader.PREFERRED_WINDOW_SIZE);
    }
    return SquashFsReader.fromMappedFile(0, mmap, cache, new DataBlockCache(64),
        new DataBlockCache(64));
  }

  @Test
  public void emptyArchiveShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 1,
          reader.getSuperBlock().getInodeCount());

      DirectoryINode root = reader.getRootInode();
      assertEquals("wrong uidIdx", (short) 0, root.getUidIdx());
      assertEquals("wrong gidIdx", (short) 0, root.getGidIdx());
      assertEquals("wrong file size", 3, root.getFileSize());
      assertEquals("wrong index count", (short) 0, root.getIndexCount());
      assertEquals("wrong inode number", 1, root.getInodeNumber());
      assertSame("wrong inode type", INodeType.BASIC_DIRECTORY,
          root.getInodeType());
      assertEquals("wrong nlink count", 2, root.getNlink());
      assertEquals("wrong permissions", (short) 0755, root.getPermissions());
      assertEquals("wrong xattr index", INode.XATTR_NOT_PRESENT,
          root.getXattrIndex());
    }
  }

  @Test
  public void archiveWithDirectoriesAtRootShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      for (int i = 0; i < 10; i++) {
        SquashFsEntry entry = writer.entry(String.format("/dir-%d", i))
            .directory()
            .lastModified(System.currentTimeMillis())
            .uid(0)
            .gid(0)
            .permissions((short) 0755)
            .build();
        System.out.println(entry);
      }
      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 11,
          reader.getSuperBlock().getInodeCount());

      for (int i = 0; i < 10; i++) {
        INode dir = reader.findInodeByPath(String.format("/dir-%d", i));
        assertSame(String.format("wrong type for entry %d", i),
            INodeType.BASIC_DIRECTORY, dir.getInodeType());
      }

      // verify links
      List<DirectoryEntry> children = reader.getChildren(reader.getRootInode());
      assertEquals("wrong directory entry count", 10, children.size());

      for (int i = 0; i < 10; i++) {
        DirectoryEntry child = children.get(i);

        assertEquals(String.format("wrong name for entry %d", i),
            String.format("dir-%d", i),
            child.getNameAsString());

        INode dir = reader.findInodeByDirectoryEntry(child);
        assertSame(String.format("wrong type for entry %d", i),
            INodeType.BASIC_DIRECTORY, dir.getInodeType());
      }
    }
  }

  @Test
  public void archiveWithSyntheticDirectoryShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dir")
          .directory()
          .synthetic()
          .uid(0)
          .gid(0)
          .permissions((short) 0755)
          .build();

      writer.entry("/dir")
          .directory()
          .uid(0)
          .gid(0)
          .permissions((short) 0711)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode dir = reader.findInodeByPath("/dir");
      assertSame("wrong type", INodeType.BASIC_DIRECTORY, dir.getInodeType());
      assertEquals("wrong permissions", (short) 0711, dir.getPermissions());
    }
  }

  @Test
  public void archiveWithMissingParentDirectoryShouldBeAutoCreated()
      throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dir/dir2/dir3")
          .directory()
          .synthetic()
          .uid(0)
          .gid(0)
          .permissions((short) 0711)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 4,
          reader.getSuperBlock().getInodeCount());

      INode dir = reader.findInodeByPath("/dir");
      assertSame("wrong parent type",
          INodeType.BASIC_DIRECTORY, dir.getInodeType());
      assertEquals("wrong parent permissions",
          (short) 0755, dir.getPermissions());

      INode dir2 = reader.findInodeByPath("/dir/dir2");
      assertSame("wrong child type",
          INodeType.BASIC_DIRECTORY, dir2.getInodeType());
      assertEquals("wrong child permissions",
          (short) 0755, dir2.getPermissions());

      INode dir3 = reader.findInodeByPath("/dir/dir2/dir3");
      assertSame("wrong grandchild type",
          INodeType.BASIC_DIRECTORY, dir3.getInodeType());
      assertEquals("wrong grandchild permissions",
          (short) 0711, dir3.getPermissions());
    }
  }

  @Test
  public void archiveWithDuplicateDirectoryShouldOverwriteContent()
      throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dir")
          .directory()
          .uid(0)
          .gid(0)
          .permissions((short) 0755)
          .build();

      writer.entry("/dir")
          .directory()
          .synthetic()
          .uid(0)
          .gid(0)
          .permissions((short) 0711)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode dir = reader.findInodeByPath("/dir");
      assertSame("wrong type", INodeType.BASIC_DIRECTORY, dir.getInodeType());
      assertEquals("wrong permissions", (short) 0711, dir.getPermissions());
    }
  }

  @Test
  public void archiveWithFilesAtRootShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      for (int i = 0; i < 10; i++) {
        writer.entry(String.format("/file-%d", i))
            .lastModified(System.currentTimeMillis())
            .uid(0)
            .gid(0)
            .content(new ByteArrayInputStream(
                String.format("file-%d", i)
                    .getBytes(StandardCharsets.ISO_8859_1)))
            .permissions((short) 0644)
            .build();
      }
      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 11,
          reader.getSuperBlock().getInodeCount());

      for (int i = 0; i < 10; i++) {
        INode file = reader.findInodeByPath(String.format("/file-%d", i));
        assertSame(String.format("Wrong type for entry %d", i),
            INodeType.BASIC_FILE, file.getInodeType());
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
          reader.writeFileStream(file, bos);
          String content =
              new String(bos.toByteArray(), StandardCharsets.ISO_8859_1);
          assertEquals(String.format("file-%d", i), content);
        }
      }
    }
  }

  @Test
  public void archiveWithDuplicateFileShouldUseLatestContent()
      throws Exception {
    File archive = temp.newFile();

    byte[] content1 = "test1".getBytes(StandardCharsets.UTF_8);
    byte[] content2 = "test2".getBytes(StandardCharsets.UTF_8);

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/full.dat")
          .lastModified(Instant.now())
          .uid(1000)
          .gid(2000)
          .content(new ByteArrayInputStream(content2))
          .permissions((short) 0644)
          .build();

      writer.entry("/full.dat")
          .lastModified(Instant.now())
          .uid(1000)
          .gid(2000)
          .content(new ByteArrayInputStream(content2))
          .permissions((short) 0644)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      INode file = reader.findInodeByPath("/full.dat");
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertEquals(
            "Wrong file content",
            "test2",
            new String(bos.toByteArray(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void archiveWithFileContainingFullBlockShouldWork() throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[131072];
    Random r = new Random(0L);
    r.nextBytes(content);

    long modified = System.currentTimeMillis();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      SquashFsEntry entry = writer.entry("/full.dat")
          .lastModified(Instant.ofEpochMilli(modified))
          .uid(1000)
          .gid(2000)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      writer.finish();

      assertEquals("wrong child size", 0, entry.getChildren().size());
      assertEquals("wrong inode number", 1, entry.getInodeNumber());
      assertEquals("wrong short name", "full.dat", entry.getShortName());
      assertEquals("wrong parent", "", entry.getParent().getName());
      assertSame("wrong inode type", INodeType.BASIC_FILE,
          entry.getInode().getInodeType());
      assertFalse("synthetic", entry.isSynthetic());
      assertEquals("wrong uid", (short) 1, entry.getUid());
      assertEquals("wrong gid", (short) 2, entry.getGid());
      assertEquals("wrong nlink count", 1, entry.getNlink());
      assertEquals("wrong file size", 131072L, entry.getFileSize());
      assertEquals("wrong last modified",
          modified, entry.getLastModified() * 1000L, 10000L);
      assertEquals("wrong data block count", 1, entry.getDataBlocks().size());
      assertNull("fragment found", entry.getFragment());
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 3,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());
      INode file = reader.findInodeByPath("/full.dat");
      assertSame("wrong type", INodeType.BASIC_FILE, file.getInodeType());
      assertEquals("wrong uid", 1000,
          reader.getIdTable().idFromIndex(file.getUidIdx()));
      assertEquals("wrong gid", 2000,
          reader.getIdTable().idFromIndex(file.getGidIdx()));
      assertEquals("wrong size", 131072L, ((FileINode) file).getFileSize());

      // test writing entire file
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

      INodeRef ref =
          reader.getExportTable().getInodeRef(file.getInodeNumber());
      INode file2 = reader.findInodeByInodeRef(ref);
      assertEquals("wrong inode number using write()", file.getInodeNumber(),
          file2.getInodeNumber());

      // test reading data
      byte[] xfer = new byte[1024];
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        int c;
        long fileOffset = 0L;
        while ((c = reader.read(file, fileOffset, xfer, 0, xfer.length))
            >= 0) {
          if (c >= 0) {
            bos.write(xfer, 0, c);
            fileOffset += c;
          }
        }
        assertArrayEquals(content, bos.toByteArray());
      }

      INodeRef refRead =
          reader.getExportTable().getInodeRef(file.getInodeNumber());
      INode inode = reader.findInodeByInodeRef(refRead);
      assertEquals("wrong inode number using read()", inode.getInodeNumber(),
          inode.getInodeNumber());
    }
  }

  @Test
  public void archiveWithFileContainingSparseBlockShouldWork()
      throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[262144];
    content[262143] = 1; // not sparse second block

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/sparse.dat")
          .lastModified(new Date())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode file = reader.findInodeByPath("/sparse.dat");

      assertSame("wrong type", INodeType.EXTENDED_FILE, file.getInodeType());
      assertEquals("wrong sparse count", 131072L,
          ((FileINode) file).getSparse());
      assertEquals("wrong size", 262144L, ((FileINode) file).getFileSize());

      // test writing data
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

      // test reading data
      byte[] xfer = new byte[1024];
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        int c;
        long fileOffset = 0L;
        while ((c = reader.read(file, fileOffset, xfer, 0, xfer.length))
            >= 0) {
          if (c >= 0) {
            bos.write(xfer, 0, c);
            fileOffset += c;
          }
        }
        assertArrayEquals(content, bos.toByteArray());
      }
    }
  }

  @Test
  public void archiveWithFileContainingAllSparseBlocksShouldWork()
      throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[262144];

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/sparse.dat")
          .lastModified(new Date())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode file = reader.findInodeByPath("/sparse.dat");

      assertSame("wrong type", INodeType.EXTENDED_FILE, file.getInodeType());
      assertEquals("wrong sparse count", 262143L,
          ((FileINode) file).getSparse());
      assertEquals("wrong size", 262144L, ((FileINode) file).getFileSize());

      // test writing data
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }
      // test reading data
      byte[] xfer = new byte[1024];
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        int c;
        long fileOffset = 0L;
        while ((c = reader.read(file, fileOffset, xfer, 0, xfer.length))
            >= 0) {
          if (c >= 0) {
            bos.write(xfer, 0, c);
            fileOffset += c;
          }
        }
        assertArrayEquals(content, bos.toByteArray());
      }
    }
  }

  @Test
  public void archiveWithHardLinkToFileShouldWork() throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[1024];
    Random r = new Random(0L);
    r.nextBytes(content);

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {

      // write first file
      SquashFsEntry target = writer.entry("/target.dat")
          .lastModified(System.currentTimeMillis())
          .uid(1)
          .gid(1)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      // write second file
      SquashFsEntry source = writer.entry("/source.dat")
          .hardlink("/target.dat")
          .build();

      writer.finish();

      assertEquals("wrong target name", "/target.dat",
          source.getHardlinkTarget());
      assertSame("wrong target", target, source.getHardlinkEntry());
      assertSame("wrong inode", target.getInode(), source.getInode());
      assertSame("wrong link count for source", 2, source.getNlink());
      assertSame("wrong link count for target", 2, target.getNlink());
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 2,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode file = reader.findInodeByPath("/target.dat");
      assertSame("wrong type", INodeType.EXTENDED_FILE, file.getInodeType());
      assertEquals("wrong size", 1024L, ((FileINode) file).getFileSize());
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

      INode file2 = reader.findInodeByPath("/source.dat");
      assertSame("wrong type", INodeType.EXTENDED_FILE, file2.getInodeType());
      assertSame("wrong inode number", file.getInodeNumber(),
          file2.getInodeNumber());
      assertEquals("wrong size", 1024L, ((FileINode) file2).getFileSize());
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

    }
  }

  @Test
  public void archiveWithFileContainingMultipleBlocksAndFragmentShouldWork()
      throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[275000];
    Random r = new Random(0L);
    r.nextBytes(content);

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/multiple.dat")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 2,
          reader.getSuperBlock().getInodeCount());

      INode file = reader.findInodeByPath("/multiple.dat");
      assertSame("wrong type", INodeType.BASIC_FILE, file.getInodeType());
      assertEquals("wrong size", 275000L, ((FileINode) file).getFileSize());

      // test writing data
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

      // test reading data
      byte[] xfer = new byte[1024];
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        int c;
        long fileOffset = 0L;
        while ((c = reader.read(file, fileOffset, xfer, 0, xfer.length))
            >= 0) {
          if (c >= 0) {
            bos.write(xfer, 0, c);
            fileOffset += c;
          }
        }
        assertArrayEquals(content, bos.toByteArray());
      }
    }
  }

  @Test
  public void archiveWithCharDeviceShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dev")
          .directory()
          .uid(0)
          .gid(0)
          .permissions((short) 0755)
          .build();

      SquashFsEntry dev = writer.entry("/dev/zero")
          .uid(0)
          .gid(0)
          .charDev(1, 5)
          .permissions((short) 0644)
          .build();

      writer.finish();

      assertEquals("wrong major", 1, dev.getMajor());
      assertEquals("wrong minor", 5, dev.getMinor());
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 3,
          reader.getSuperBlock().getInodeCount());

      INode dev = reader.findInodeByPath("/dev/zero");
      assertSame("wrong type", INodeType.BASIC_CHAR_DEVICE, dev.getInodeType());
      assertEquals("wrong device", (1 << 8) | 5,
          ((DeviceINode) dev).getDevice());
    }
  }

  @Test
  public void archiveWithBlockDeviceShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dev")
          .directory()
          .uid(0)
          .gid(0)
          .permissions((short) 0755)
          .build();

      SquashFsEntry dev = writer.entry("/dev/loop0")
          .uid(0)
          .gid(0)
          .blockDev(7, 0)
          .permissions((short) 0644)
          .build();

      writer.finish();

      assertEquals("wrong major", 7, dev.getMajor());
      assertEquals("wrong minor", 0, dev.getMinor());
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 3,
          reader.getSuperBlock().getInodeCount());

      INode dev = reader.findInodeByPath("/dev/loop0");
      assertSame("wrong type", INodeType.BASIC_BLOCK_DEVICE,
          dev.getInodeType());
      assertEquals("wrong device", (7 << 8), ((DeviceINode) dev).getDevice());
    }
  }

  @Test
  public void archiveWithFifoShouldWork() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/dev")
          .directory()
          .uid(0)
          .gid(0)
          .permissions((short) 0755)
          .build();

      writer.entry("/dev/log")
          .uid(0)
          .gid(0)
          .fifo()
          .permissions((short) 0666)
          .build();

      writer.finish();
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 3,
          reader.getSuperBlock().getInodeCount());

      INode dev = reader.findInodeByPath("/dev/log");
      assertSame("wrong type", INodeType.BASIC_FIFO, dev.getInodeType());
      assertEquals("wrong permissions", (short) 0666, dev.getPermissions());
    }
  }

  @Test
  public void archiveWithSymlinkShouldWork() throws Exception {
    File archive = temp.newFile();

    byte[] content = new byte[1024];
    Random r = new Random(0L);
    r.nextBytes(content);

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/target.dat")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(content))
          .permissions((short) 0644)
          .build();

      SquashFsEntry link = writer.entry("/link.dat")
          .uid(0)
          .gid(0)
          .symlink("/target.dat")
          .permissions((short) 0644)
          .build();

      writer.finish();

      assertEquals("wrong link target", "/target.dat", link.getSymlinkTarget());
    }

    try (SquashFsReader reader = createReader(archive)) {
      assertEquals("wrong id count", (short) 1,
          reader.getSuperBlock().getIdCount());
      assertEquals("wrong inode count", 3,
          reader.getSuperBlock().getInodeCount());

      INode link = reader.findInodeByPath("/link.dat");
      assertSame("wrong type", INodeType.BASIC_SYMLINK, link.getInodeType());
      String target = new String(((SymlinkINode) link).getTargetPath(),
          StandardCharsets.ISO_8859_1);

      INode file = reader.findInodeByPath(target);
      assertSame("wrong type", INodeType.BASIC_FILE, file.getInodeType());
      assertEquals("wrong size", 1024, ((FileINode) file).getFileSize());
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
        reader.writeFileStream(file, bos);
        assertArrayEquals(content, bos.toByteArray());
      }

    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNullNameShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry(null)
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithEmptyNameShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNonAbsoluteNameShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("foo")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNameEndingInSlashShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/foo/")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNoTypeShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/no-type.dat")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNoUidShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/no-uid.dat")
          .lastModified(System.currentTimeMillis())
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNoGidShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/no-gid.dat")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNoPermissionsShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/no-permissions.dat")
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .build();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileWithNoFileSizeShouldFail() throws Exception {
    File archive = temp.newFile();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      writer.entry("/no-size.dat")
          .file()
          .lastModified(System.currentTimeMillis())
          .uid(0)
          .gid(0)
          .permissions((short) 0644)
          .build();
    }
  }

  @Test
  public void fileWithNoTimestampShouldUseDefault() throws Exception {
    File archive = temp.newFile();

    long timestamp = System.currentTimeMillis();

    try (SquashFsWriter writer = new SquashFsWriter(archive)) {
      SquashFsEntry entry = writer.entry("/no-timestamp.dat")
          .uid(0)
          .gid(0)
          .content(new ByteArrayInputStream(new byte[0]))
          .permissions((short) 0644)
          .build();

      long actualTimestamp = entry.getLastModified() * 1000L;

      assertEquals("wrong timestamp", (double) timestamp,
          (double) (entry.getLastModified() * 1000d), 60_000d);
    }
  }

}
