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

import org.apache.hadoop.runc.squashfs.data.DataBlockRef;
import org.apache.hadoop.runc.squashfs.data.FragmentRef;
import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.LongConsumer;

public class SquashFsEntryBuilder {

  private static final Logger LOG =
      LoggerFactory.getLogger(SquashFsEntryBuilder.class);

  private final SquashFsWriter writer;

  private INodeType type;
  private int major;
  private int minor;
  private String name;
  private Short uid;
  private Short gid;
  private Short permissions;
  private Long fileSize;
  private Integer lastModified;
  private String symlinkTarget;
  private String hardlinkTarget;
  private List<DataBlockRef> dataBlocks;
  private FragmentRef fragment;
  private boolean synthetic = false;

  public SquashFsEntryBuilder(SquashFsWriter writer, String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("filename is required");
    }
    if (!name.startsWith("/")) {
      throw new IllegalArgumentException(
          String.format("filename '%s' must begin with a slash", name));
    }
    if (name.length() > 1 && name.endsWith("/")) {
      throw new IllegalArgumentException(
          String.format("filename '%s' may not end with a slash", name));
    }
    if ("/".equals(name)) {
      name = "";
    }
    this.writer = writer;
    this.name = name;
  }

  public SquashFsEntryBuilder uid(int value) {
    this.uid = writer.getIdGenerator().addUidGid(value);
    return this;
  }

  public SquashFsEntryBuilder gid(int value) {
    this.gid = writer.getIdGenerator().addUidGid(value);
    return this;
  }

  public SquashFsEntryBuilder dataBlock(DataBlockRef value) {
    if (dataBlocks == null) {
      dataBlocks = new ArrayList<>();
    }
    LOG.debug("Wrote datablock {}", value);
    dataBlocks.add(value);
    return this;
  }

  public SquashFsEntryBuilder fragment(FragmentRef value) {
    this.fragment = value;
    LOG.debug("Wrote fragment {}", value);
    return this;
  }

  public SquashFsEntryBuilder permissions(short value) {
    this.permissions = value;
    return this;
  }

  public SquashFsEntryBuilder lastModified(Date value) {
    return lastModified(value.getTime());
  }

  public SquashFsEntryBuilder lastModified(Instant value) {
    return lastModified(value.toEpochMilli());
  }

  public SquashFsEntryBuilder lastModified(long value) {
    this.lastModified = (int) (value / 1000);
    return this;
  }

  public SquashFsEntryBuilder fileSize(long value) {
    this.fileSize = value;
    return this;
  }

  public SquashFsEntryBuilder directory() {
    this.type = INodeType.BASIC_DIRECTORY;
    return this;
  }

  public SquashFsEntryBuilder file() {
    this.type = INodeType.BASIC_FILE;
    return this;
  }

  public SquashFsEntryBuilder blockDev(int majorNum, int minorNum) {
    this.type = INodeType.BASIC_BLOCK_DEVICE;
    this.major = majorNum;
    this.minor = minorNum;
    return this;
  }

  public SquashFsEntryBuilder charDev(int majorNum, int minorNum) {
    this.type = INodeType.BASIC_CHAR_DEVICE;
    this.major = majorNum;
    this.minor = minorNum;
    return this;
  }

  public SquashFsEntryBuilder fifo() {
    this.type = INodeType.BASIC_FIFO;
    return this;
  }

  public SquashFsEntryBuilder symlink(String target) {
    this.type = INodeType.BASIC_SYMLINK;
    this.symlinkTarget = target;
    return this;
  }

  public SquashFsEntryBuilder hardlink(String target) {
    this.hardlinkTarget = target;
    return this;
  }

  public SquashFsEntryBuilder synthetic() {
    this.synthetic = true;
    return this;
  }

  public SquashFsEntryBuilder content(byte[] content) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(content)) {
      return content(bis, content.length);
    }
  }

  public SquashFsEntryBuilder content(InputStream in) throws IOException {
    return content(in, Long.MAX_VALUE);
  }

  public SquashFsEntryBuilder content(InputStream in, long maxSize)
      throws IOException {
    return content(in, maxSize, l -> {
    });
  }

  public SquashFsEntryBuilder content(InputStream in, long maxSize,
      LongConsumer progress) throws IOException {
    long written = 0L;
    int c = 0;
    int off = 0;

    byte[] blockBuffer = writer.getBlockBuffer();

    // determine how many bytes to read
    int bytesToRead =
        (int) Math.min(blockBuffer.length - off, maxSize - written);
    while (c >= 0 && bytesToRead > 0) {

      // attempt to read full block
      while (bytesToRead > 0
          && (c = in.read(blockBuffer, off, bytesToRead)) >= 0) {
        off += c;
        written += c;
        if (off == blockBuffer.length) {
          // write the block
          LOG.trace("Writing block of size {}", blockBuffer.length);
          DataBlockRef dataBlock =
              writer.getDataWriter().write(blockBuffer, 0, blockBuffer.length);
          dataBlock(dataBlock);
          progress.accept(written);
          off = 0;
          c = 0;
          break;
        }
        bytesToRead =
            (int) Math.min(blockBuffer.length - off, maxSize - written);
      }
    }

    if (off > 0) {
      // write final block
      LOG.trace("Writing fragment of size {}", off);
      FragmentRef fref =
          writer.getFragmentWriter().write(blockBuffer, 0, off);
      fragment(fref);
      progress.accept(written);
      off = 0;
    }

    LOG.debug("Wrote {} bytes to {}", written, name);

    // set output type to file
    if (type == null) {
      file();
    }

    // set output file size
    if (fileSize == null) {
      fileSize(written);
    }

    return this;
  }

  public SquashFsEntry build() {
    if (type == null && hardlinkTarget == null) {
      throw new IllegalArgumentException("type not set");
    }
    if (uid == null && hardlinkTarget == null) {
      throw new IllegalArgumentException("uid not set");
    }
    if (gid == null && hardlinkTarget == null) {
      throw new IllegalArgumentException("gid not set");
    }
    if (permissions == null && hardlinkTarget == null) {
      throw new IllegalArgumentException("permissions not set");
    }
    if (fileSize == null && type == INodeType.BASIC_FILE) {
      throw new IllegalArgumentException("fileSize not set");
    }
    if (lastModified == null && hardlinkTarget == null) {
      lastModified = (int) (System.currentTimeMillis() / 1000L);
    }

    SquashFsEntry entry = new SquashFsEntry(
        type,
        name,
        Optional.ofNullable(uid).orElse((short) 0),
        Optional.ofNullable(gid).orElse((short) 0),
        Optional.ofNullable(permissions).orElse((short) 0),
        major,
        minor,
        Optional.ofNullable(fileSize).orElse(0L),
        Optional.ofNullable(lastModified).orElse(0),
        symlinkTarget,
        hardlinkTarget,
        dataBlocks,
        fragment,
        synthetic);

    writer.getFsTree().add(entry);

    return entry;
  }

}
