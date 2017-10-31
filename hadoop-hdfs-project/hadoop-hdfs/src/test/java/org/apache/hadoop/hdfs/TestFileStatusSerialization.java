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
package org.apache.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.fs.FSProtos.FileStatusProto;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.google.protobuf.ByteString;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Verify compatible FileStatus/HdfsFileStatus serialization.
 */
public class TestFileStatusSerialization {

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
  }

  /**
   * Test API backwards-compatibility with 2.x applications w.r.t. FsPermission.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testFsPermissionCompatibility() throws Exception {
    final int flagmask = 0x8;
    // flags compatible with 2.x; fixed as constant in this test to ensure
    // compatibility is maintained. New flags are not part of the contract this
    // test verifies.
    for (int i = 0; i < flagmask; ++i) {
      FsPermission perm = FsPermission.createImmutable((short) 0013);
      HdfsFileStatusProto.Builder hspb = HdfsFileStatusProto.newBuilder()
          .setFileType(FileType.IS_FILE)
          .setPath(ByteString.copyFromUtf8("hdfs://foobar/dingos/zot"))
          .setLength(4344)
          .setPermission(PBHelperClient.convert(perm))
          .setOwner("hadoop")
          .setGroup("unqbbc")
          .setModificationTime(12345678L)
          .setAccessTime(87654321L)
          .setBlockReplication(10)
          .setBlocksize(1L << 33)
          .setFlags(i);
      HdfsFileStatus stat = PBHelperClient.convert(hspb.build());
      stat.makeQualified(new URI("hdfs://foobar"), new Path("/dingos"));
      assertEquals(new Path("hdfs://foobar/dingos/zot"), stat.getPath());

      // verify deprecated FsPermissionExtension methods
      FsPermission sp = stat.getPermission();
      assertEquals(sp.getAclBit(), stat.hasAcl());
      assertEquals(sp.getEncryptedBit(), stat.isEncrypted());
      assertEquals(sp.getErasureCodedBit(), stat.isErasureCoded());

      // verify Writable contract
      DataOutputBuffer dob = new DataOutputBuffer();
      stat.write(dob);
      DataInputBuffer dib = new DataInputBuffer();
      dib.reset(dob.getData(), 0, dob.getLength());
      FileStatus fstat = new FileStatus();
      fstat.readFields(dib);
      checkFields(stat, fstat);

      // FsPermisisonExtension used for HdfsFileStatus, not FileStatus,
      // attribute flags should still be preserved
      assertEquals(sp.getAclBit(), fstat.hasAcl());
      assertEquals(sp.getEncryptedBit(), fstat.isEncrypted());
      assertEquals(sp.getErasureCodedBit(), fstat.isErasureCoded());
    }
  }
  // param for LocatedFileStatus, HttpFileStatus

  @Test
  public void testCrossSerializationProto() throws Exception {
    FsPermission perm = FsPermission.getFileDefault();
    for (FileType t : FileType.values()) {
      HdfsFileStatusProto.Builder hspb = HdfsFileStatusProto.newBuilder()
          .setFileType(t)
          .setPath(ByteString.copyFromUtf8("hdfs://foobar/dingos"))
          .setLength(4344)
          .setPermission(PBHelperClient.convert(perm))
          .setOwner("hadoop")
          .setGroup("unqbbc")
          .setModificationTime(12345678L)
          .setAccessTime(87654321L)
          .setBlockReplication(10)
          .setBlocksize(1L << 33);
      if (FileType.IS_SYMLINK.equals(t)) {
        hspb.setSymlink(ByteString.copyFromUtf8("hdfs://yaks/dingos"));
      }
      if (FileType.IS_FILE.equals(t)) {
        hspb.setFileId(4544);
      }
      HdfsFileStatusProto hsp = hspb.build();
      byte[] src = hsp.toByteArray();
      FileStatusProto fsp = FileStatusProto.parseFrom(src);
      assertEquals(hsp.getPath().toStringUtf8(), fsp.getPath());
      assertEquals(hsp.getLength(), fsp.getLength());
      assertEquals(hsp.getPermission().getPerm(),
                   fsp.getPermission().getPerm());
      assertEquals(hsp.getOwner(), fsp.getOwner());
      assertEquals(hsp.getGroup(), fsp.getGroup());
      assertEquals(hsp.getModificationTime(), fsp.getModificationTime());
      assertEquals(hsp.getAccessTime(), fsp.getAccessTime());
      assertEquals(hsp.getSymlink().toStringUtf8(), fsp.getSymlink());
      assertEquals(hsp.getBlockReplication(), fsp.getBlockReplication());
      assertEquals(hsp.getBlocksize(), fsp.getBlockSize());
      assertEquals(hsp.getFileType().ordinal(), fsp.getFileType().ordinal());

      // verify unknown fields preserved
      byte[] dst = fsp.toByteArray();
      HdfsFileStatusProto hsp2 = HdfsFileStatusProto.parseFrom(dst);
      assertEquals(hsp, hsp2);
      checkFields(PBHelperClient.convert(hsp), PBHelperClient.convert(hsp2));
    }
  }

}
