/**
 *
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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for EncryptionZoneManager methods. Added tests for
 * listEncryptionZones method, for cases where inode can and cannot have a
 * parent inode.
 */
public class TestEncryptionZoneManager {

  private FSDirectory mockedDir;
  private INodesInPath mockedINodesInPath;
  private INodeDirectory firstINode;
  private INodeDirectory secondINode;
  private INodeDirectory rootINode;
  private PermissionStatus defaultPermission;
  private EncryptionZoneManager ezManager;

  @Before
  public void setup() {
    this.mockedDir = mock(FSDirectory.class);
    this.mockedINodesInPath = mock(INodesInPath.class);
    this.defaultPermission = new PermissionStatus("test", "test",
      new FsPermission((short) 755));
    this.rootINode =
        new INodeDirectory(0L, "".getBytes(), defaultPermission,
          System.currentTimeMillis());
    this.firstINode =
        new INodeDirectory(1L, "first".getBytes(), defaultPermission,
          System.currentTimeMillis());
    this.secondINode =
        new INodeDirectory(2L, "second".getBytes(), defaultPermission,
          System.currentTimeMillis());
    when(this.mockedDir.hasReadLock()).thenReturn(true);
    when(this.mockedDir.hasWriteLock()).thenReturn(true);
    when(this.mockedDir.getInode(0L)).thenReturn(rootINode);
    when(this.mockedDir.getInode(1L)).thenReturn(firstINode);
    when(this.mockedDir.getInode(2L)).thenReturn(secondINode);
  }

  @Test
  public void testListEncryptionZonesOneValidOnly() throws Exception{
    this.ezManager = new EncryptionZoneManager(mockedDir, new Configuration());
    this.ezManager.addEncryptionZone(1L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    this.ezManager.addEncryptionZone(2L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    // sets root as proper parent for firstINode only
    this.firstINode.setParent(rootINode);
    when(mockedDir.getINodesInPath("/first", DirOp.READ_LINK)).
        thenReturn(mockedINodesInPath);
    when(mockedINodesInPath.getLastINode()).
        thenReturn(firstINode);
    BatchedListEntries<EncryptionZone> result = ezManager.
        listEncryptionZones(0);
    assertEquals(1, result.size());
    assertEquals(1L, result.get(0).getId());
    assertEquals("/first", result.get(0).getPath());
  }

  @Test
  public void testListEncryptionZonesTwoValids() throws Exception {
    this.ezManager = new EncryptionZoneManager(mockedDir, new Configuration());
    this.ezManager.addEncryptionZone(1L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    this.ezManager.addEncryptionZone(2L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    // sets root as proper parent for both inodes
    this.firstINode.setParent(rootINode);
    this.secondINode.setParent(rootINode);
    when(mockedDir.getINodesInPath("/first", DirOp.READ_LINK)).
        thenReturn(mockedINodesInPath);
    when(mockedINodesInPath.getLastINode()).
        thenReturn(firstINode);
    INodesInPath mockedINodesInPathForSecond =
        mock(INodesInPath.class);
    when(mockedDir.getINodesInPath("/second", DirOp.READ_LINK)).
        thenReturn(mockedINodesInPathForSecond);
    when(mockedINodesInPathForSecond.getLastINode()).
        thenReturn(secondINode);
    BatchedListEntries<EncryptionZone> result =
        ezManager.listEncryptionZones(0);
    assertEquals(2, result.size());
    assertEquals(1L, result.get(0).getId());
    assertEquals("/first", result.get(0).getPath());
    assertEquals(2L, result.get(1).getId());
    assertEquals("/second", result.get(1).getPath());
  }

  @Test
  public void testListEncryptionZonesForRoot() throws Exception{
    this.ezManager = new EncryptionZoneManager(mockedDir, new Configuration());
    this.ezManager.addEncryptionZone(0L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    // sets root as proper parent for firstINode only
    when(mockedDir.getINodesInPath("/", DirOp.READ_LINK)).
        thenReturn(mockedINodesInPath);
    when(mockedINodesInPath.getLastINode()).
        thenReturn(rootINode);
    BatchedListEntries<EncryptionZone> result = ezManager.
        listEncryptionZones(-1);
    assertEquals(1, result.size());
    assertEquals(0L, result.get(0).getId());
    assertEquals("/", result.get(0).getPath());
  }

  @Test
  public void testListEncryptionZonesSubDirInvalid() throws Exception{
    INodeDirectory thirdINode = new INodeDirectory(3L, "third".getBytes(),
        defaultPermission, System.currentTimeMillis());
    when(this.mockedDir.getInode(3L)).thenReturn(thirdINode);
    //sets "second" as parent
    thirdINode.setParent(this.secondINode);
    this.ezManager = new EncryptionZoneManager(mockedDir, new Configuration());
    this.ezManager.addEncryptionZone(1L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    this.ezManager.addEncryptionZone(3L, CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, "test_key");
    // sets root as proper parent for firstINode only,
    // leave secondINode with no parent
    this.firstINode.setParent(rootINode);
    when(mockedDir.getINodesInPath("/first", DirOp.READ_LINK)).
        thenReturn(mockedINodesInPath);
    when(mockedINodesInPath.getLastINode()).
        thenReturn(firstINode);
    BatchedListEntries<EncryptionZone> result = ezManager.
        listEncryptionZones(0);
    assertEquals(1, result.size());
    assertEquals(1L, result.get(0).getId());
    assertEquals("/first", result.get(0).getPath());
  }
}
