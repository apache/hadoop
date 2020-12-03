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

package org.apache.hadoop.runc.squashfs.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.runc.squashfs.MappedSquashFsReader;
import org.apache.hadoop.runc.squashfs.SquashFsReader;
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.io.MappedFile;
import org.apache.hadoop.runc.squashfs.metadata.MetadataReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class SquashDebug extends Configured implements Tool {

  private SquashFsReader createReader(
      File file, boolean mapped) throws IOException {
    if (mapped) {
      System.out.println("Using memory-mapped reader");
      System.out.println();
      try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
        try (FileChannel channel = raf.getChannel()) {
          MappedFile mmap = MappedFile.mmap(channel,
              MappedSquashFsReader.PREFERRED_MAP_SIZE,
              MappedSquashFsReader.PREFERRED_WINDOW_SIZE);

          return SquashFsReader.fromMappedFile(0, mmap);
        }
      }
    } else {
      System.out.println("Using file reader");
      System.out.println();
      return SquashFsReader.fromFile(0, file);
    }
  }

  private void dumpTree(SquashFsReader reader, boolean readFiles)
      throws IOException {
    System.out.println("Directory tree:");
    System.out.println();
    DirectoryINode root = reader.getRootInode();
    dumpSubtree(reader, true, "/", root, readFiles);
  }

  private void dumpFileContent(SquashFsReader reader, FileINode inode)
      throws IOException {
    long fileSize = inode.getFileSize();
    long readSize;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      reader.writeFileStream(inode, bos);
      byte[] content = bos.toByteArray();
      readSize = content.length;
    }
    System.out.printf("  %d bytes, %d read%n", fileSize, readSize);
  }

  private void dumpSubtree(SquashFsReader reader, boolean root,
      String path, DirectoryINode inode,
      boolean readFiles)
      throws IOException {

    if (root) {
      System.out.printf("/ (%d)%n", inode.getInodeNumber());
    }

    for (DirectoryEntry entry : reader.getChildren(inode)) {
      INode childInode = reader.findInodeByDirectoryEntry(entry);
      System.out.printf("%s%s%s (%d)%n",
          path, entry.getNameAsString(),
          childInode.getInodeType().directory() ? "/" : "",
          childInode.getInodeNumber());

      if (readFiles && childInode.getInodeType().file()) {
        dumpFileContent(reader, (FileINode) childInode);
      }
    }

    for (DirectoryEntry entry : reader.getChildren(inode)) {
      INode childInode = reader.findInodeByDirectoryEntry(entry);
      if (childInode.getInodeType().directory()) {
        dumpSubtree(reader, false, String.format("%s%s/",
            path, entry.getNameAsString()), (DirectoryINode) childInode,
            readFiles);
      }
    }
  }

  private void dumpMetadataBlock(
      SquashFsReader reader, long metaFileOffset, int metaBlockOffset)
      throws IOException {

    System.out.println();
    System.out.printf("Dumping block at file offset %d, block offset %d%n",
        metaFileOffset, metaBlockOffset);
    System.out.println();

    MetadataReader mr = reader.getMetaReader()
        .rawReader(0, metaFileOffset, (short) metaBlockOffset);
    mr.isEof(); // make sure block is read
    byte[] buf = new byte[mr.available()];
    mr.readFully(buf);

    StringBuilder sb = new StringBuilder();
    BinUtils.dumpBin(sb, 0, "data", buf, 0, buf.length, 32, 2);
    System.out.println(sb.toString());
  }

  @Override
  public int run(String[] argv) throws Exception {
    Options options = options();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
          "Error parsing command-line options: " + e.getMessage());
      printUsage();
      return -1;
    }

    if (cmd.hasOption("h")) {
      printUsage();
      return -1;
    }

    boolean mapped = false;
    boolean tree = false;
    boolean files = false;
    boolean metadata = false;
    long metaFileOffset = 0L;
    int metaBlockOffset = 0;
    String squashFs = null;

    for (Option o : cmd.getOptions()) {
      switch (o.getOpt()) {
      case "p":
        mapped = true;
        break;
      case "t":
        tree = true;
        break;
      case "f":
        files = true;
        break;
      case "m":
        metadata = true;
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown option: " + o.getOpt());
      }
    }

    String[] rem = cmd.getArgs();

    if (metadata) {
      if (rem.length != 3) {
        printUsage();
        return -1;
      }
      metaFileOffset = Long.parseLong(rem[1]);
      metaBlockOffset = Integer.parseInt(rem[2]);
    } else {
      if (rem.length != 1) {
        printUsage();
        return -1;
      }
    }

    squashFs = rem[0];

    try (SquashFsReader reader = createReader(new File(squashFs), mapped)) {
      System.out.println(reader.getSuperBlock());
      System.out.println();
      System.out.println(reader.getIdTable());
      System.out.println();
      System.out.println(reader.getFragmentTable());
      System.out.println();
      System.out.println(reader.getExportTable());
      System.out.println();

      if (tree || files) {
        dumpTree(reader, files);
      }

      if (metadata) {
        dumpMetadataBlock(reader, metaFileOffset, metaBlockOffset);
      }
    }
    return 0;
  }

  protected void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        "squashdebug [OPTIONS] <squashfs-file> [file-offset] [block-offset]",
        new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("Options", options());
    ToolRunner.printGenericCommandUsage(System.out);
  }

  static Options options() {
    Options options = new Options();
    options.addOption("p", "mmap", false, "Use mmap() for I/O");
    options.addOption("t", "tree", false, "Dump tree");
    options.addOption("f", "files", false, "Read all files (implies --tree)");
    options.addOption(
        "m", "metadata", false,
        "Dump metadata (requires file-offset and block-offset)");
    options.addOption("h", "help", false, "Print usage");
    return options;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new SquashDebug(), argv);
    System.exit(ret);
  }

}
