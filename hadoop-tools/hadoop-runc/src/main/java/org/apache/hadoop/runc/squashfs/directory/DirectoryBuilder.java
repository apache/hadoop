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

import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DirectoryBuilder {

  boolean dirty = false;
  final List<Entry> entries = new ArrayList<>();
  final List<DirectoryElement> elements = new ArrayList<>();

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
      header.count++;

      DirectoryEntry dent = new DirectoryEntry();
      dent.header = header;
      dent.offset = entry.offset;
      dent.inodeNumberDelta = (short) (entry.inodeNumber - header.inodeNumber);
      dent.name = entry.name;
      dent.type = entry.type;
      dent.size = (short) (entry.name.length - 1);

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
        (header.startBlock == entry.startBlock) &&
        (entry.inodeNumber >= header.inodeNumber) &&
        (entry.inodeNumber <= (header.inodeNumber + 0x7fff)) &&
        (header.count < (DirectoryHeader.MAX_DIR_ENTRIES - 1))) {
      return header;
    }

    header = new DirectoryHeader();
    header.count = -1;
    header.startBlock = entry.startBlock;
    header.inodeNumber = entry.inodeNumber;
    elements.add(header);
    return header;
  }

  static class Entry {
    int startBlock;
    int inodeNumber;
    byte[] name;
    short offset;
    short type;

    Entry(int startBlock, int inodeNumber, short offset, short type,
        byte[] name) {
      this.startBlock = startBlock;
      this.inodeNumber = inodeNumber;
      this.offset = offset;
      this.type = type;
      this.name = name;
    }
  }

}
