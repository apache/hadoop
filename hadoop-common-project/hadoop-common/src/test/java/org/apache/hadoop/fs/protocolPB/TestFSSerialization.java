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
package org.apache.hadoop.fs.protocolPB;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import static org.apache.hadoop.fs.FSProtos.*;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Verify PB serialization of FS data structures.
 */
public class TestFSSerialization {

  @Test
  @SuppressWarnings("deprecation")
  public void testWritableFlagSerialization() throws Exception {
    final Path p = new Path("hdfs://yaks:4344/dingos/f");
    for (int i = 0; i < 0x8; ++i) {
      final boolean acl   = 0 != (i & 0x1);
      final boolean crypt = 0 != (i & 0x2);
      final boolean ec    = 0 != (i & 0x4);
      FileStatus stat = new FileStatus(1024L, false, 3, 1L << 31,
          12345678L, 87654321L, FsPermission.getFileDefault(),
          "hadoop", "unqbbc", null, p, acl, crypt, ec);
      DataOutputBuffer dob = new DataOutputBuffer();
      stat.write(dob);
      DataInputBuffer dib = new DataInputBuffer();
      dib.reset(dob.getData(), 0, dob.getLength());
      FileStatus fstat = new FileStatus();
      fstat.readFields(dib);
      assertEquals(stat, fstat);
      checkFields(stat, fstat);
    }
  }

  @Test
  public void testUtilitySerialization() throws Exception {
    final Path p = new Path("hdfs://yaks:4344/dingos/f");
    FileStatus stat = new FileStatus(1024L, false, 3, 1L << 31,
        12345678L, 87654321L, FsPermission.createImmutable((short)0111),
        "hadoop", "unqbbc", null, p);
    FileStatusProto fsp = PBHelper.convert(stat);
    FileStatus stat2 = PBHelper.convert(fsp);
    assertEquals(stat, stat2);
    checkFields(stat, stat2);
  }

  private static void checkFields(FileStatus expected, FileStatus actual) {
    assertEquals(expected.getPath(), actual.getPath());
    assertEquals(expected.isDirectory(), actual.isDirectory());
    assertEquals(expected.getLen(), actual.getLen());
    assertEquals(expected.getPermission(), actual.getPermission());
    assertEquals(expected.getOwner(), actual.getOwner());
    assertEquals(expected.getGroup(), actual.getGroup());
    assertEquals(expected.getModificationTime(), actual.getModificationTime());
    assertEquals(expected.getAccessTime(), actual.getAccessTime());
    assertEquals(expected.getReplication(), actual.getReplication());
    assertEquals(expected.getBlockSize(), actual.getBlockSize());
    assertEquals(expected.hasAcl(), actual.hasAcl());
    assertEquals(expected.isEncrypted(), actual.isEncrypted());
    assertEquals(expected.isErasureCoded(), actual.isErasureCoded());
  }

}
