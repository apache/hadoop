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
import org.apache.hadoop.runc.squashfs.directory.DirectoryBuilder;
import org.apache.hadoop.runc.squashfs.inode.DeviceINode;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedBlockDeviceINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedCharDeviceINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedDirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedFifoINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedFileINode;
import org.apache.hadoop.runc.squashfs.inode.ExtendedSymlinkINode;
import org.apache.hadoop.runc.squashfs.inode.FifoINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.inode.Permission;
import org.apache.hadoop.runc.squashfs.inode.SymlinkINode;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SquashFsEntry {
  private final List<SquashFsEntry> children = new ArrayList<>();
  private int inodeNumber;
  private SquashFsEntry parent;
  private INodeType type;
  private INode inode;
  private String name;
  private boolean synthetic;
  private short uid;
  private short gid;
  private short permissions;
  private int major;
  private int minor;
  private int nlink;
  private long fileSize;
  private int lastModified;
  private String symlinkTarget;
  private String hardlinkTarget;
  private SquashFsEntry hardlinkEntry;
  private List<DataBlockRef> dataBlocks;
  private FragmentRef fragment;

  SquashFsEntry() {
    this.type = INodeType.BASIC_DIRECTORY;
    this.name = "";
    this.uid = 0;
    this.gid = 0;
    this.permissions = 0755;
    this.nlink = 1;
    this.lastModified = 0;
  }

  SquashFsEntry(
      INodeType type,
      String name,
      short uid,
      short gid,
      short permissions,
      int major,
      int minor,
      long fileSize,
      int lastModified,
      String symlinkTarget,
      String hardlinkTarget,
      List<DataBlockRef> dataBlocks,
      FragmentRef fragment,
      boolean synthetic) {
    this.type = type;
    this.name = name;
    this.uid = uid;
    this.gid = gid;
    this.permissions = permissions;
    this.major = major;
    this.minor = minor;
    this.nlink = 1;
    this.fileSize = fileSize;
    this.lastModified = lastModified;
    this.symlinkTarget = symlinkTarget;
    this.hardlinkTarget = hardlinkTarget;
    this.dataBlocks = dataBlocks;
    this.fragment = fragment;
    this.synthetic = synthetic;
  }

  static int compareBytes(byte[] left, byte[] right) {
    for (int i = 0; i < left.length && i < right.length; i++) {
      int a = (left[i] & 0xff);
      int b = (right[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }

  static int compareEntries(SquashFsEntry left, SquashFsEntry right) {
    return compareBytes(
        left.getShortName().getBytes(StandardCharsets.ISO_8859_1),
        right.getShortName().getBytes(StandardCharsets.ISO_8859_1));
  }

  public int getInodeNumber() {
    return inodeNumber;
  }

  public SquashFsEntry getParent() {
    return parent;
  }

  void setParent(SquashFsEntry parent) {
    this.parent = parent;
  }

  public INodeType getType() {
    return type;
  }

  public INode getInode() {
    return inode;
  }

  public String getName() {
    return name;
  }

  void setName(String name) {
    this.name = name;
  }

  public boolean isSynthetic() {
    return synthetic;
  }

  public short getUid() {
    return uid;
  }

  public short getGid() {
    return gid;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getNlink() {
    return nlink;
  }

  public long getFileSize() {
    return fileSize;
  }

  public int getLastModified() {
    return lastModified;
  }

  public String getSymlinkTarget() {
    return symlinkTarget;
  }

  public String getHardlinkTarget() {
    return hardlinkTarget;
  }

  public SquashFsEntry getHardlinkEntry() {
    return hardlinkEntry;
  }

  public List<DataBlockRef> getDataBlocks() {
    return Collections.unmodifiableList(dataBlocks);
  }

  public FragmentRef getFragment() {
    return fragment;
  }

  public String getShortName() {
    return name.substring(name.lastIndexOf('/') + 1);
  }

  public List<SquashFsEntry> getChildren() {
    return Collections.unmodifiableList(children);
  }

  void addChild(SquashFsEntry entry) {
    children.add(entry);
  }

  void sortChildren() {
    Collections.sort(children, SquashFsEntry::compareEntries);
    for (SquashFsEntry child : children) {
      child.sortChildren();
    }
  }

  void assignInodes(Map<String, SquashFsEntry> entryMap,
      AtomicInteger inodeAssignments) {
    for (SquashFsEntry child : children) {
      if (child.hardlinkTarget == null) {
        child.inodeNumber = inodeAssignments.incrementAndGet();
      }
    }
    for (SquashFsEntry child : children) {
      child.assignInodes(entryMap, inodeAssignments);
    }
    if (parent == null) {
      inodeNumber = inodeAssignments.incrementAndGet();
    }
  }

  void assignHardlinkInodes(
      SortedMap<String, SquashFsEntry> entryMap,
      SortedMap<Integer, Set<SquashFsEntry>> inodeToEntry) {
    if (hardlinkTarget != null) {
      SquashFsEntry target = entryMap.get(hardlinkTarget);
      hardlinkEntry = target;
      inodeNumber = target.inodeNumber;
      Integer key = Integer.valueOf(inodeNumber);
      if (!inodeToEntry.containsKey(key)) {
        inodeToEntry.put(key, new LinkedHashSet<>());
      }
      inodeToEntry.get(key).add(target);
      inodeToEntry.get(key).add(this);
    }
    for (SquashFsEntry child : children) {
      child.assignHardlinkInodes(entryMap, inodeToEntry);
    }
  }

  void updateDirectoryLinkCounts() {
    if (type != null && type.directory()) {
      nlink++;
    }

    for (SquashFsEntry child : children) {
      nlink++;
      child.updateDirectoryLinkCounts();
    }
  }

  void updateHardlinkInodeCounts(
      SortedMap<Integer, Set<SquashFsEntry>> inodeToEntry) {
    for (Set<SquashFsEntry> set : inodeToEntry.values()) {
      int count = set.stream().mapToInt(e -> e.nlink).sum();
      set.stream().forEach(e -> e.nlink = count);
    }
  }

  void createInodes() {
    for (SquashFsEntry child : children) {
      if (child.hardlinkTarget == null) {
        child.inode = child.createINode();
      }
    }
    for (SquashFsEntry child : children) {
      child.createInodes();
    }
    if (parent == null) {
      inode = createINode();
    }
  }

  void createHardlinkInodes() {
    if (hardlinkEntry != null) {
      this.inode = hardlinkEntry.inode;
    }
    for (SquashFsEntry child : children) {
      child.createHardlinkInodes();
    }
  }

  MetadataBlockRef writeMetadata(
      MetadataWriter inodeWriter,
      MetadataWriter dirWriter,
      Map<Integer, MetadataBlockRef> visitedInodes) throws IOException {

    if (type != null && type.directory()) {
      if (children.isEmpty()) {
        DirectoryINode dirInode = (DirectoryINode) inode;
        dirInode.setFileSize(3);
        dirInode.setStartBlock(0);
        dirInode.setOffset((short) 0);
      } else {
        for (SquashFsEntry child : children) {
          child.writeMetadata(inodeWriter, dirWriter, visitedInodes);
        }

        DirectoryBuilder db = new DirectoryBuilder();

        for (SquashFsEntry child : children) {
          Integer inodeKey = Integer.valueOf(child.inodeNumber);

          MetadataBlockRef inodeRef;
          if (visitedInodes.containsKey(inodeKey)) {
            inodeRef = visitedInodes.get(inodeKey);
          } else {
            child.inode = child.inode.simplify();
            inodeRef = inodeWriter.getCurrentReference();
            child.inode.writeData(inodeWriter);
            visitedInodes.put(inodeKey, inodeRef);
          }

          db.add(
              child.getShortName(),
              inodeRef.getLocation(),
              child.inodeNumber,
              inodeRef.getOffset(),
              child.inode.getInodeType());
        }

        MetadataBlockRef dirRef = dirWriter.getCurrentReference();
        db.write(dirWriter);

        int size = db.getStructureSize();
        DirectoryINode dirInode = (DirectoryINode) inode;
        dirInode.setFileSize(size + 3);
        dirInode.setStartBlock(dirRef.getLocation());
        dirInode.setOffset(dirRef.getOffset());
      }

      if (parent == null) {
        // root
        MetadataBlockRef rootInodeRef = inodeWriter.getCurrentReference();
        visitedInodes.put(
            Integer.valueOf(inode.getInodeNumber()), rootInodeRef);

        DirectoryINode rootInode = (DirectoryINode) inode;
        rootInode.setParentInodeNumber(visitedInodes.size() + 1);
        inode = rootInode.simplify();
        inode.writeData(inodeWriter);

        return rootInodeRef;
      }
    }

    return null;
  }

  private INode createINode() {
    switch (type) {
    case BASIC_DIRECTORY:
      return createDirectoryINode();
    case BASIC_FILE:
      return createFileINode();
    case BASIC_BLOCK_DEVICE:
      return createBlockDevice();
    case BASIC_CHAR_DEVICE:
      return createCharDevice();
    case BASIC_FIFO:
      return createFifo();
    case BASIC_SYMLINK:
      return createSymlink();
    default:
      throw new IllegalArgumentException(
          String.format("Invalid inode type %s", type));
    }
  }

  private <T extends INode> T fill(T otherInode) {
    otherInode.setInodeNumber(inodeNumber);
    otherInode.setUidIdx(uid);
    otherInode.setGidIdx(gid);
    otherInode.setPermissions(permissions);
    otherInode.setModifiedTime(lastModified);
    return otherInode;
  }

  private DirectoryINode createDirectoryINode() {
    ExtendedDirectoryINode dir = new ExtendedDirectoryINode();

    dir.setParentInodeNumber(parent == null ? -1 : parent.inodeNumber);
    dir.setNlink(nlink);
    return fill(dir);
  }

  private FileINode createFileINode() {
    ExtendedFileINode file = new ExtendedFileINode();
    file.setFileSize(fileSize);
    file.setNlink(nlink);

    if (dataBlocks == null || dataBlocks.isEmpty()) {
      file.setBlocksStart(0L);
      file.setBlockSizes(new int[0]);
      file.setSparse(0L);
    } else {
      long sparse = 0L;
      file.setBlocksStart(dataBlocks.get(0).getLocation());
      int[] sizes = new int[dataBlocks.size()];
      for (int i = 0; i < sizes.length; i++) {
        DataBlockRef dbr = dataBlocks.get(i);
        if (dbr.isSparse()) {
          sparse += dbr.getLogicalSize();
          sizes[i] = 0;
        } else {
          sizes[i] = dbr.getPhysicalSize();
          if (!dbr.isCompressed()) {
            sizes[i] |= 0x1_000_000; // uncompressed bit
          }
        }
      }
      if (sparse >= fileSize) {
        sparse = fileSize - 1L;
      }
      file.setBlockSizes(sizes);
      file.setSparse(sparse);
    }

    if (fragment != null) {
      file.setFragmentBlockIndex(fragment.getFragmentIndex());
      file.setFragmentOffset(fragment.getOffset());
    }

    return fill(file);
  }

  private DeviceINode createBlockDevice() {
    ExtendedBlockDeviceINode dev = new ExtendedBlockDeviceINode();

    dev.setNlink(nlink);
    dev.setDevice(deviceNum());

    return fill(dev);
  }

  private DeviceINode createCharDevice() {
    ExtendedCharDeviceINode dev = new ExtendedCharDeviceINode();

    dev.setNlink(nlink);
    dev.setDevice(deviceNum());

    return fill(dev);
  }

  private FifoINode createFifo() {
    ExtendedFifoINode fifo = new ExtendedFifoINode();
    fifo.setNlink(nlink);
    return fill(fifo);
  }

  private SymlinkINode createSymlink() {
    ExtendedSymlinkINode symlink = new ExtendedSymlinkINode();
    symlink.setNlink(nlink);
    symlink.setTargetPath(symlinkTarget.getBytes(StandardCharsets.ISO_8859_1));
    return fill(symlink);
  }

  private int deviceNum() {
    long deviceNum = 0L;
    deviceNum |= ((major & 0xfff) << 8);
    deviceNum |= (minor & 0xff);
    deviceNum |= ((minor & 0xfff00) << 12);

    return (int) (deviceNum & 0xffffffff);
  }

  @Override
  public String toString() {
    return String.format("%s%s %5d %5d %5d %5d %10d %s %s%s%s%s",
        type.mode(),
        Permission.toDisplay(permissions),
        uid,
        gid,
        inodeNumber,
        nlink,
        fileSize,
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            .format(lastModified * 1000L),
        (parent == null && inode != null) ? "/" : "",
        name,
        hardlinkTarget == null ? "" : " link to " + hardlinkTarget,
        symlinkTarget == null ? "" : " -> " + symlinkTarget);
  }

}
