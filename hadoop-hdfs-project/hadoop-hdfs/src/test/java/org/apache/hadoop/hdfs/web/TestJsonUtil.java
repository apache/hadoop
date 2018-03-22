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
package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Lists;

public class TestJsonUtil {

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(Map.class);

  static FileStatus toFileStatus(HdfsFileStatus f, String parent) {
    return new FileStatus(f.getLen(), f.isDirectory(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink()
          ? new Path(DFSUtilClient.bytes2String(f.getSymlinkInBytes()))
          : null,
        new Path(f.getFullName(parent)));
  }

  @Test
  public void testHdfsFileStatus() throws IOException {
    final long now = Time.now();
    final String parent = "/dir";
    final HdfsFileStatus status = new HdfsFileStatus.Builder()
        .length(1001L)
        .replication(3)
        .blocksize(1L << 26)
        .mtime(now)
        .atime(now + 10)
        .perm(new FsPermission((short) 0644))
        .owner("user")
        .group("group")
        .symlink(DFSUtil.string2Bytes("bar"))
        .path(DFSUtil.string2Bytes("foo"))
        .fileId(HdfsConstants.GRANDFATHER_INODE_ID)
        .build();
    final FileStatus fstatus = toFileStatus(status, parent);
    System.out.println("status  = " + status);
    System.out.println("fstatus = " + fstatus);
    final String json = JsonUtil.toJsonString(status, true);
    System.out.println("json    = " + json.replace(",", ",\n  "));
    final HdfsFileStatus s2 =
        JsonUtilClient.toFileStatus((Map<?, ?>) READER.readValue(json), true);
    final FileStatus fs2 = toFileStatus(s2, parent);
    System.out.println("s2      = " + s2);
    System.out.println("fs2     = " + fs2);
    Assert.assertEquals(fstatus, fs2);
  }
  
  @Test
  public void testToDatanodeInfoWithoutSecurePort() throws Exception {
    Map<String, Object> response = new HashMap<String, Object>();
    
    response.put("ipAddr", "127.0.0.1");
    response.put("hostName", "localhost");
    response.put("storageID", "fake-id");
    response.put("xferPort", 1337l);
    response.put("infoPort", 1338l);
    // deliberately don't include an entry for "infoSecurePort"
    response.put("ipcPort", 1339l);
    response.put("capacity", 1024l);
    response.put("dfsUsed", 512l);
    response.put("remaining", 512l);
    response.put("blockPoolUsed", 512l);
    response.put("lastUpdate", 0l);
    response.put("xceiverCount", 4096l);
    response.put("networkLocation", "foo.bar.baz");
    response.put("adminState", "NORMAL");
    response.put("cacheCapacity", 123l);
    response.put("cacheUsed", 321l);
    
    JsonUtilClient.toDatanodeInfo(response);
  }

  @Test
  public void testToDatanodeInfoWithName() throws Exception {
    Map<String, Object> response = new HashMap<String, Object>();

    // Older servers (1.x, 0.23, etc.) sends 'name' instead of ipAddr
    // and xferPort.
    String name = "127.0.0.1:1004";
    response.put("name", name);
    response.put("hostName", "localhost");
    response.put("storageID", "fake-id");
    response.put("infoPort", 1338l);
    response.put("ipcPort", 1339l);
    response.put("capacity", 1024l);
    response.put("dfsUsed", 512l);
    response.put("remaining", 512l);
    response.put("blockPoolUsed", 512l);
    response.put("lastUpdate", 0l);
    response.put("xceiverCount", 4096l);
    response.put("networkLocation", "foo.bar.baz");
    response.put("adminState", "NORMAL");
    response.put("cacheCapacity", 123l);
    response.put("cacheUsed", 321l);

    DatanodeInfo di = JsonUtilClient.toDatanodeInfo(response);
    Assert.assertEquals(name, di.getXferAddr());

    // The encoded result should contain name, ipAddr and xferPort.
    Map<String, Object> r = JsonUtil.toJsonMap(di);
    Assert.assertEquals(name, r.get("name"));
    Assert.assertEquals("127.0.0.1", r.get("ipAddr"));
    // In this test, it is Integer instead of Long since json was not actually
    // involved in constructing the map.
    Assert.assertEquals(1004, (int)(Integer)r.get("xferPort"));

    // Invalid names
    String[] badNames = {"127.0.0.1", "127.0.0.1:", ":", "127.0.0.1:sweet", ":123"};
    for (String badName : badNames) {
      response.put("name", badName);
      checkDecodeFailure(response);
    }

    // Missing both name and ipAddr
    response.remove("name");
    checkDecodeFailure(response);

    // Only missing xferPort
    response.put("ipAddr", "127.0.0.1");
    checkDecodeFailure(response);
  }
  
  @Test
  public void testToAclStatus() throws IOException {
    String jsonString =
        "{\"AclStatus\":{\"entries\":[\"user::rwx\",\"user:user1:rw-\",\"group::rw-\",\"other::r-x\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
    Map<?, ?> json = READER.readValue(jsonString);

    List<AclEntry> aclSpec =
        Lists.newArrayList(aclEntry(ACCESS, USER, ALL),
            aclEntry(ACCESS, USER, "user1", READ_WRITE),
            aclEntry(ACCESS, GROUP, READ_WRITE),
            aclEntry(ACCESS, OTHER, READ_EXECUTE));

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner("testuser");
    aclStatusBuilder.group("supergroup");
    aclStatusBuilder.addEntries(aclSpec);
    aclStatusBuilder.stickyBit(false);

    Assert.assertEquals("Should be equal", aclStatusBuilder.build(),
        JsonUtilClient.toAclStatus(json));
  }

  @Test
  public void testToJsonFromAclStatus() {
    String jsonString =
        "{\"AclStatus\":{\"entries\":[\"user:user1:rwx\",\"group::rw-\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner("testuser");
    aclStatusBuilder.group("supergroup");
    aclStatusBuilder.stickyBit(false);

    List<AclEntry> aclSpec =
        Lists.newArrayList(aclEntry(ACCESS, USER,"user1", ALL),
            aclEntry(ACCESS, GROUP, READ_WRITE));

    aclStatusBuilder.addEntries(aclSpec);
    Assert.assertEquals(jsonString,
        JsonUtil.toJsonString(aclStatusBuilder.build()));

  }
  
  @Test
  public void testToJsonFromXAttrs() throws IOException {
    String jsonString = 
        "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"}," +
        "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
    XAttr xAttr1 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(XAttrCodec.decodeValue("0x313233")).build();
    XAttr xAttr2 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a2").setValue(XAttrCodec.decodeValue("0x313131")).build();
    List<XAttr> xAttrs = Lists.newArrayList();
    xAttrs.add(xAttr1);
    xAttrs.add(xAttr2);
    
    Assert.assertEquals(jsonString, JsonUtil.toJsonString(xAttrs, 
        XAttrCodec.HEX));
  }
  
  @Test
  public void testToXAttrMap() throws IOException {
    String jsonString = 
        "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"}," +
        "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
    Map<?, ?> json = READER.readValue(jsonString);
    XAttr xAttr1 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(XAttrCodec.decodeValue("0x313233")).build();
    XAttr xAttr2 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a2").setValue(XAttrCodec.decodeValue("0x313131")).build();
    List<XAttr> xAttrs = Lists.newArrayList();
    xAttrs.add(xAttr1);
    xAttrs.add(xAttr2);
    Map<String, byte[]> xAttrMap = XAttrHelper.buildXAttrMap(xAttrs);
    Map<String, byte[]> parsedXAttrMap = JsonUtilClient.toXAttrs(json);
    
    Assert.assertEquals(xAttrMap.size(), parsedXAttrMap.size());
    Iterator<Entry<String, byte[]>> iter = xAttrMap.entrySet().iterator();
    while(iter.hasNext()) {
      Entry<String, byte[]> entry = iter.next();
      Assert.assertArrayEquals(entry.getValue(), 
          parsedXAttrMap.get(entry.getKey()));
    }
  }
  
  @Test
  public void testGetXAttrFromJson() throws IOException {
    String jsonString = 
        "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"}," +
        "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
    Map<?, ?> json = READER.readValue(jsonString);

    // Get xattr: user.a2
    byte[] value = JsonUtilClient.getXAttr(json, "user.a2");
    Assert.assertArrayEquals(XAttrCodec.decodeValue("0x313131"), value);
  }

  private void checkDecodeFailure(Map<String, Object> map) {
    try {
      JsonUtilClient.toDatanodeInfo(map);
      Assert.fail("Exception not thrown against bad input.");
    } catch (Exception e) {
      // expected
    }
  }
}
