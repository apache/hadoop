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
package org.apache.hadoop.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verify CopyListingFileStatus serialization and requirements for distcp.
 */
public class TestCopyListingFileStatus {

  @Test
  public void testToString() {
    CopyListingFileStatus src = new CopyListingFileStatus(
        4344L, false, 2, 512 << 20, 1234L, 5678L, new FsPermission((short)0512),
        "dingo", "yaks", new Path("hdfs://localhost:4344"));
    src.toString();
    src = new CopyListingFileStatus();
    src.toString();
  }

  @Test
  public void testCopyListingFileStatusSerialization() throws Exception {
    CopyListingFileStatus src = new CopyListingFileStatus(
        4344L, false, 2, 512 << 20, 1234L, 5678L, new FsPermission((short)0512),
        "dingo", "yaks", new Path("hdfs://localhost:4344"));
    DataOutputBuffer dob = new DataOutputBuffer();
    src.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    CopyListingFileStatus dst = new CopyListingFileStatus();
    dst.readFields(dib);
    assertEquals(src, dst);
  }

  @Test
  public void testFileStatusEquality() throws Exception {
    FileStatus stat = new FileStatus(
        4344L, false, 2, 512 << 20, 1234L, 5678L, new FsPermission((short)0512),
        "dingo", "yaks", new Path("hdfs://localhost:4344/foo/bar/baz"));
    CopyListingFileStatus clfs = new CopyListingFileStatus(stat);
    assertEquals(stat.getLen(), clfs.getLen());
    assertEquals(stat.isDirectory(), clfs.isDirectory());
    assertEquals(stat.getReplication(), clfs.getReplication());
    assertEquals(stat.getBlockSize(), clfs.getBlockSize());
    assertEquals(stat.getAccessTime(), clfs.getAccessTime());
    assertEquals(stat.getModificationTime(), clfs.getModificationTime());
    assertEquals(stat.getPermission(), clfs.getPermission());
    assertEquals(stat.getOwner(), clfs.getOwner());
    assertEquals(stat.getGroup(), clfs.getGroup());
    assertEquals(stat.getPath(), clfs.getPath());
    assertEquals(stat.isErasureCoded(), clfs.isErasureCoded());
  }

  @Test
  public void testAclEntriesAndXAttrsAreCopied() {
    CopyListingFileStatus status = new CopyListingFileStatus(
        4344L, false, 2, 512 << 20, 1234L, 5678L, new FsPermission((short)0750),
        "dingo", "yaks", new Path("hdfs://localhost:4344"));

    List<AclEntry> aclEntries = new ArrayList<>();
    aclEntries.add(aclEntry("user1"));
    status.setAclEntries(aclEntries);
    List<AclEntry> expectedAcl = status.getAclEntries();
    aclEntries.add(aclEntry("user2"));
    assertEquals(expectedAcl, status.getAclEntries(),
        "changing the list passed to setAclEntries() changed the status");

    Map<String, byte[]> xAttrs = new HashMap<>();
    byte[] value = new byte[] {1, 2, 3};
    xAttrs.put("user.a", value);
    status.setXAttrs(xAttrs);
    xAttrs.put("user.b", new byte[] {4});
    assertEquals(1, status.getXAttrs().size(),
        "changing the map passed to setXAttrs() changed the status");
    assertSame(value, status.getXAttrs().get("user.a"));

    status.setAclEntries(null);
    status.setXAttrs(null);
    assertTrue(status.getXAttrs().isEmpty());
  }

  private static AclEntry aclEntry(String user) {
    return new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setName(user)
        .setPermission(FsAction.READ)
        .build();
  }
}
