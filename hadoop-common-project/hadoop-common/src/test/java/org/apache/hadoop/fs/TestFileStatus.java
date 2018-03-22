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
package org.apache.hadoop.fs;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileStatus {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileStatus.class);
  
  /** Values for creating {@link FileStatus} in some tests */
  static final int LENGTH = 1;
  static final int REPLICATION = 2;
  static final long BLKSIZE = 3;
  static final long MTIME = 4;
  static final long ATIME = 5;
  static final String OWNER = "owner";
  static final String GROUP = "group";
  static final FsPermission PERMISSION = FsPermission.valueOf("-rw-rw-rw-");
  static final Path PATH = new Path("path");

  /**
   * Check that the write and readField methods work correctly.
   */
  @Test
  public void testFileStatusWritable() throws Exception {
    FileStatus[] tests = {
        new FileStatus(1,false,5,3,4,5,null,"","",new Path("/a/b")),
        new FileStatus(0,false,1,2,3,new Path("/")),
        new FileStatus(1,false,5,3,4,5,null,"","",new Path("/a/b"))
      };

    LOG.info("Writing FileStatuses to a ByteArrayOutputStream");
    // Writing input list to ByteArrayOutputStream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    for (FileStatus fs : tests) {
      fs.write(out);
    }

    LOG.info("Creating ByteArrayInputStream object");
    DataInput in =
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));


    LOG.info("Testing if read objects are equal to written ones");
    FileStatus dest = new FileStatus();
    int iterator = 0;
    for (FileStatus fs : tests) {
      dest.readFields(in);
      assertEquals("Different FileStatuses in iteration " + iterator,
          dest, fs);
      iterator++;
    }
  }
  
  /**
   * Check that the full parameter constructor works correctly.
   */
  @Test
  public void constructorFull() throws IOException {
    boolean isdir = false;
    Path symlink = new Path("symlink");
    FileStatus fileStatus = new FileStatus(LENGTH, isdir, REPLICATION, BLKSIZE,
        MTIME, ATIME, PERMISSION, OWNER, GROUP, symlink, PATH);
    
    validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME,
      ATIME, PERMISSION, OWNER, GROUP, symlink, PATH);
  }

  /**
   * Check that the non-symlink constructor works correctly.
   */
  @Test
  public void constructorNoSymlink() throws IOException {
    boolean isdir = true;  
    FileStatus fileStatus = new FileStatus(LENGTH, isdir, REPLICATION, BLKSIZE,
        MTIME, ATIME, PERMISSION, OWNER, GROUP, PATH);
    validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME,
        ATIME, PERMISSION, OWNER, GROUP, null, PATH);
  }

  /**
   * Check that the constructor without owner, group and permissions works
   * correctly.
   */
  @Test
  public void constructorNoOwner() throws IOException {
    boolean isdir = true;    
    FileStatus fileStatus = new FileStatus(LENGTH, isdir,
        REPLICATION, BLKSIZE, MTIME, PATH);   
    validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME,
        0, FsPermission.getDirDefault(), "", "", null, PATH);
  }

  /**
   * Check that the no parameter constructor works correctly.
   */
  @Test
  public void constructorBlank() throws IOException {
    FileStatus fileStatus = new FileStatus();  
    validateAccessors(fileStatus, 0, false, 0, 0, 0,
        0, FsPermission.getFileDefault(), "", "", null, null);
  }

  /**
   * Check that FileStatus are equal if their paths are equal.
   */
  @Test
  public void testEquals() {
    Path path = new Path("path");
    FileStatus fileStatus1 = new FileStatus(1, true, 1, 1, 1, 1,
        FsPermission.valueOf("-rw-rw-rw-"), "one", "one", null, path);
    FileStatus fileStatus2 = new FileStatus(2, true, 2, 2, 2, 2,
        FsPermission.valueOf("---x--x--x"), "two", "two", null, path);
    assertEquals(fileStatus1, fileStatus2);
  }

  /**
   * Check that FileStatus are not equal if their paths are not equal.
   */
  @Test
  public void testNotEquals() {
    Path path1 = new Path("path1");
    Path path2 = new Path("path2");
    FileStatus fileStatus1 = new FileStatus(1, true, 1, 1, 1, 1,
        FsPermission.valueOf("-rw-rw-rw-"), "one", "one", null, path1);
    FileStatus fileStatus2 = new FileStatus(1, true, 1, 1, 1, 1,
        FsPermission.valueOf("-rw-rw-rw-"), "one", "one", null, path2);
    assertFalse(fileStatus1.equals(fileStatus2));
    assertFalse(fileStatus2.equals(fileStatus1));
  }

  /**
   * Check that toString produces the expected output for a file.
   */
  @Test
  public void toStringFile() throws IOException {
    boolean isdir = false; 
    FileStatus fileStatus = new FileStatus(LENGTH, isdir, REPLICATION, BLKSIZE,
        MTIME, ATIME, PERMISSION, OWNER, GROUP, null, PATH);   
    validateToString(fileStatus);
  }
  
  /**
   * Check that toString produces the expected output for a directory.
   */
  @Test
  public void toStringDir() throws IOException {
    FileStatus fileStatus = new FileStatus(LENGTH, true, REPLICATION, BLKSIZE,
        MTIME, ATIME, PERMISSION, OWNER, GROUP, null, PATH); 
    validateToString(fileStatus);
  }
  
  @Test
  public void testCompareTo() throws IOException {
    Path path1 = new Path("path1");
    Path path2 = new Path("path2");
    FileStatus fileStatus1 =
        new FileStatus(1, true, 1, 1, 1, 1, FsPermission.valueOf("-rw-rw-rw-"),
            "one", "one", null, path1);
    FileStatus fileStatus2 =
        new FileStatus(1, true, 1, 1, 1, 1, FsPermission.valueOf("-rw-rw-rw-"),
            "one", "one", null, path2);
    assertTrue(fileStatus1.compareTo(fileStatus2) < 0);
    assertTrue(fileStatus2.compareTo(fileStatus1) > 0);

    List<FileStatus> statList = new ArrayList<>();
    statList.add(fileStatus1);
    statList.add(fileStatus2);
    assertTrue(Collections.binarySearch(statList, fileStatus1) > -1);
  }

  /**
   * Check that toString produces the expected output for a symlink.
   */
  @Test
  public void toStringSymlink() throws IOException {
    boolean isdir = false;
    Path symlink = new Path("symlink");
    FileStatus fileStatus = new FileStatus(LENGTH, isdir, REPLICATION, BLKSIZE,
        MTIME, ATIME, PERMISSION, OWNER, GROUP, symlink, PATH);  
    validateToString(fileStatus);
  }

  @Test
  public void testSerializable() throws Exception {
    Path p = new Path("uqsf://ybpnyubfg:8020/sbb/one/onm");
    FsPermission perm = FsPermission.getFileDefault();
    FileStatus stat = new FileStatus(4344L, false, 4, 512L << 20, 12345678L,
        87654321L, perm, "yak", "dingo", p);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(stat);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      FileStatus deser = (FileStatus) ois.readObject();
      assertEquals(stat, deser);
    }
  }
  
  /**
   * Validate the accessors for FileStatus.
   * @param fileStatus FileStatus to checked
   * @param length expected length
   * @param isdir expected isDirectory
   * @param replication expected replication
   * @param blocksize expected blocksize
   * @param mtime expected modification time
   * @param atime expected access time
   * @param permission expected permission
   * @param owner expected owner
   * @param group expected group
   * @param symlink expected symlink
   * @param path expected path
   */
  private void validateAccessors(FileStatus fileStatus,
    long length, boolean isdir, int replication, long blocksize, long mtime,
    long atime, FsPermission permission, String owner, String group,
    Path symlink, Path path) throws IOException {
    
    assertEquals(length, fileStatus.getLen());
    assertEquals(isdir, fileStatus.isDirectory());
    assertEquals(replication, fileStatus.getReplication());
    assertEquals(blocksize, fileStatus.getBlockSize());
    assertEquals(mtime, fileStatus.getModificationTime());
    assertEquals(atime, fileStatus.getAccessTime());
    assertEquals(permission, fileStatus.getPermission());
    assertEquals(owner, fileStatus.getOwner());
    assertEquals(group, fileStatus.getGroup());
    if(symlink == null) {
      assertFalse(fileStatus.isSymlink());
    } else {
      assertTrue(fileStatus.isSymlink());
      assertEquals(symlink, fileStatus.getSymlink());
    }
    assertEquals(path, fileStatus.getPath());
  }

  /**
   * Validates the toString method for FileStatus.
   * @param fileStatus FileStatus to be validated
   */
  private void validateToString(FileStatus fileStatus) throws IOException {
    StringBuilder expected = new StringBuilder();
    expected.append("FileStatus{");
    expected.append("path=").append(fileStatus.getPath()).append("; ");
    expected.append("isDirectory=").append(fileStatus.isDirectory()).append("; ");
    if(!fileStatus.isDirectory()) {
      expected.append("length=").append(fileStatus.getLen()).append("; ");
      expected.append("replication=").append(fileStatus.getReplication()).append("; ");
      expected.append("blocksize=").append(fileStatus.getBlockSize()).append("; ");
    }
    expected.append("modification_time=").append(fileStatus.getModificationTime()).append("; ");
    expected.append("access_time=").append(fileStatus.getAccessTime()).append("; ");
    expected.append("owner=").append(fileStatus.getOwner()).append("; ");
    expected.append("group=").append(fileStatus.getGroup()).append("; ");
    expected.append("permission=").append(fileStatus.getPermission()).append("; ");
    if(fileStatus.isSymlink()) {
      expected.append("isSymlink=").append(true).append("; ");
      expected.append("symlink=").append(fileStatus.getSymlink()).append("; ");
    } else {
      expected.append("isSymlink=").append(false).append("; ");
    }
    expected.append("hasAcl=").append(fileStatus.hasAcl()).append("; ");
    expected.append("isEncrypted=").append(
        fileStatus.isEncrypted()).append("; ");
    expected.append("isErasureCoded=").append(
        fileStatus.isErasureCoded()).append("}");
    assertEquals(expected.toString(), fileStatus.toString());
  }
}
