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

import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.table.ExportTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SquashFsTree {

  private final SortedMap<String, SquashFsEntry> map = new TreeMap<>();

  private final AtomicInteger inodeAssignments = new AtomicInteger(0);
  private final SortedMap<Integer, Set<SquashFsEntry>> inodeToEntry =
      new TreeMap<>();
  private final MetadataWriter inodeWriter = new MetadataWriter();
  private final MetadataWriter dirWriter = new MetadataWriter();
  private final SortedMap<Integer, MetadataBlockRef> visitedInodes =
      new TreeMap<>();

  private SquashFsEntry root = new SquashFsEntry();
  private MetadataBlockRef rootInodeRef;

  SquashFsTree() {

  }

  void add(SquashFsEntry squashFsEntry) {
    if (squashFsEntry.getName().isEmpty()) {
      if (squashFsEntry.getType() != INodeType.BASIC_DIRECTORY) {
        throw new IllegalArgumentException("Root entry must be a directory");
      }
      this.root = squashFsEntry;
    } else {
      map.put(squashFsEntry.getName(), squashFsEntry);
    }
  }

  public SquashFsEntry getRoot() {
    return root;
  }

  void build() throws SquashFsException, IOException {

    // synthesize missing parents
    SortedMap<String, SquashFsEntry> existing = new TreeMap<>(map);
    for (Map.Entry<String, SquashFsEntry> squashFsEntry : existing.entrySet()) {
      String name = squashFsEntry.getKey();
      String parent = name;
      while ((parent = parentName(parent)) != null) {
        SquashFsEntry p = map.get(parent);
        if (p == null) {
          // synthesize an entry
          p = new SquashFsEntry();
          p.setName(parent);
          map.put(parent, p);
        } else if (p.getType() != INodeType.BASIC_DIRECTORY) {
          throw new IllegalArgumentException(String.format(
              "Parent '%s' of entry '%s' is not a directory",
              parent, name));
        }
      }
    }

    for (Map.Entry<String, SquashFsEntry> squashFsEntry : map.entrySet()) {
      String name = squashFsEntry.getKey();
      String hardLinkTarget = squashFsEntry.getValue().getHardlinkTarget();
      if (hardLinkTarget != null && !map.containsKey(hardLinkTarget)) {
        throw new IllegalArgumentException(
            String.format("Hardlink target '%s' not found for entry '%s'",
                hardLinkTarget, name));
      }

      // assign parent
      String parent = parentName(name);
      if (parent == null) {
        root.addChild(squashFsEntry.getValue());
        squashFsEntry.getValue().setParent(root);
      } else {
        SquashFsEntry parentEntry = map.get(parent);
        parentEntry.addChild(squashFsEntry.getValue());
        squashFsEntry.getValue().setParent(parentEntry);
      }
    }

    // walk tree, sort entries and assign inodes
    root.sortChildren();

    root.assignInodes(map, inodeAssignments);
    root.assignHardlinkInodes(map, inodeToEntry);

    root.updateDirectoryLinkCounts();
    root.updateHardlinkInodeCounts(inodeToEntry);

    root.createInodes();
    root.createHardlinkInodes();

    rootInodeRef = root.writeMetadata(inodeWriter, dirWriter, visitedInodes);

    // make sure all inodes were visited
    if (visitedInodes.size() != root.getInode().getInodeNumber()) {
      throw new SquashFsException(
          String.format("BUG: Visited inode count %d != actual inode count %d",
              visitedInodes.size(), root.getInode().getInodeNumber()));
    }

    // make sure all inode numbers exist, from 1 to n
    List<Integer> allInodes =
        visitedInodes.keySet().stream().collect(Collectors.toList());
    int firstInode = allInodes.get(0).intValue();
    if (firstInode != 1) {
      throw new SquashFsException(String.format(
          "BUG: First inode number %d != 1", firstInode));
    }
    int lastInode = allInodes.get(allInodes.size() - 1).intValue();
    if (lastInode != allInodes.size()) {
      throw new SquashFsException(String.format(
          "BUG: Last inode number %d != %d", lastInode, allInodes.size()));
    }
  }

  int getInodeCount() {
    return visitedInodes.size();
  }

  List<MetadataBlockRef> saveExportTable(MetadataWriter writer)
      throws IOException {

    List<MetadataBlockRef> exportRefs = new ArrayList<>();

    int index = 0;
    for (Map.Entry<Integer, MetadataBlockRef> entry : visitedInodes
        .entrySet()) {
      if (index % ExportTable.ENTRIES_PER_BLOCK == 0) {
        exportRefs.add(writer.getCurrentReference());
      }
      MetadataBlockRef metaRef = entry.getValue();

      long inodeRef = (((long) (metaRef.getLocation() & 0xffffffffL)) << 16) |
          (((long) metaRef.getOffset()) & 0xffffL);

      writer.writeLong(inodeRef);
      index++;
    }

    return exportRefs;
  }

  MetadataBlockRef getRootInodeRef() {
    return rootInodeRef;
  }

  MetadataWriter getINodeWriter() {
    return inodeWriter;
  }

  MetadataWriter getDirWriter() {
    return dirWriter;
  }

  private String parentName(String name) {
    int slash = name.lastIndexOf('/');
    if (slash <= 0) {
      return null;
    }
    return name.substring(0, slash);
  }

}
