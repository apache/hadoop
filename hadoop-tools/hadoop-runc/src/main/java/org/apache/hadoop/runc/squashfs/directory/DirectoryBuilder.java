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

package org.apache.hadoop.runc.squashfs.directory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DirectoryBuilder {

  private boolean dirty = false;
  private final List<Entry> entries = new ArrayList<>();
  private final List<DirectoryElement> elements = new ArrayList<>();

  public void add(
      String name,
      int startBlock,
      int inodeNumber,
      short offset,
      INodeType type) {
    dirty = true;
    byte[] nameBytes = name.getBytes(StandardCharsets.ISO_8859_1);
    if (nameBytes.length < 1) {
      throw new IllegalArgumentException("Filename is empty");
    }
    if (nameBytes.length > ((int) DirectoryEntry.MAX_FILENAME_LENGTH)) {
      throw new IllegalArgumentException(String.format(
          "Filename '%s' too long (%d bytes, max %d)",
          name, nameBytes.length, DirectoryEntry.MAX_FILENAME_LENGTH));
    }
    entries.add(new Entry(
        startBlock, inodeNumber, offset, type.dirValue(), nameBytes));
  }

  @VisibleForTesting
  List<Entry> getEntries() {
    return Collections.unmodifiableList(entries);
  }

  @VisibleForTesting
  List<DirectoryElement> getElements() {
    return Collections.unmodifiableList(elements);
  }

  public int getStructureSize() {
    build();
    int size = 0;
    for (DirectoryElement element : elements) {
      size += element.getStructureSize();
    }
    return size;
  }

  void build() {
    if (!dirty) {
      return;
    }
    elements.clear();

    DirectoryHeader header = null;
    for (Entry entry : entries) {
      header = advance(header, entry);
      header.incrementCount();

      DirectoryEntry dent = new DirectoryEntry(
          entry.offset,
          (short) (entry.inodeNumber - header.getInodeNumber()),
          entry.getType(),
          (short) (entry.getName().length - 1),
          entry.getName(),
          null);

      elements.add(dent);
    }
    dirty = false;
  }

  public void write(MetadataWriter out) throws IOException {
    build();
    for (DirectoryElement element : elements) {
      element.writeData(out);
    }
  }

  private DirectoryHeader advance(DirectoryHeader header, Entry entry) {
    if ((header != null) &&
        (header.getStartBlock() == entry.startBlock) &&
        (entry.inodeNumber >= header.getInodeNumber()) &&
        (entry.inodeNumber <= (header.getInodeNumber() + 0x7fff)) &&
        (header.getCount() < (DirectoryHeader.MAX_DIR_ENTRIES - 1))) {
      return header;
    }

    header = new DirectoryHeader(-1, entry.startBlock, entry.inodeNumber);
    elements.add(header);
    return header;
  }

  public static class Entry {
    private final int startBlock;
    private final int inodeNumber;
    private final byte[] name;
    private final short offset;
    private final short type;

    public Entry(int startBlock, int inodeNumber, short offset, short type,
        byte[] name) {
      this.startBlock = startBlock;
      this.inodeNumber = inodeNumber;
      this.offset = offset;
      this.type = type;
      this.name = name;
    }

    public int getStartBlock() {
      return startBlock;
    }

    public int getInodeNumber() {
      return inodeNumber;
    }

    public byte[] getName() {
      return name;
    }

    public short getOffset() {
      return offset;
    }

    public short getType() {
      return type;
    }
  }

}
