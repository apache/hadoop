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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

import org.junit.Test;

public class TestINodeFile {

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private String userName = "Test";
  private short replication;
  private long preferredBlockSize;

  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testReplicationBelowLowerBound ()
              throws IllegalArgumentException {
    replication = -1;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null,
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null,
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", preferredBlockSize,
           inf.getPreferredBlockSize());
  }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
                 inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = -1;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  } 

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE+1;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  }

  @Test
  public void testGetFullPathName() {
    PermissionStatus perms = new PermissionStatus(
      userName, null, FsPermission.getDefault());

    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(perms, null, replication,
                                  0L, 0L, preferredBlockSize);
    inf.setLocalName("f");

    INodeDirectory root = new INodeDirectory(INodeDirectory.ROOT_NAME, perms);
    INodeDirectory dir = new INodeDirectory("d", perms);

    assertEquals("f", inf.getFullPathName());
    assertEquals("", inf.getLocalParentDir());

    dir.addChild(inf, false);
    assertEquals("d"+Path.SEPARATOR+"f", inf.getFullPathName());
    assertEquals("d", inf.getLocalParentDir());
    
    root.addChild(dir, false);
    assertEquals(Path.SEPARATOR+"d"+Path.SEPARATOR+"f", inf.getFullPathName());
    assertEquals(Path.SEPARATOR+"d", dir.getFullPathName());

    assertEquals(Path.SEPARATOR, root.getFullPathName());
    assertEquals(Path.SEPARATOR, root.getLocalParentDir());
    
  }
  
  @Test
  public void testAppendBlocks() {
    INodeFile origFile = createINodeFiles(1, "origfile")[0];
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 1L);

    INodeFile[] appendFiles =   createINodeFiles(4, "appendfile");
    origFile.appendBlocks(appendFiles, getTotalBlocks(appendFiles));
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 5L);
    
    for(int i=0; i< origFile.numBlocks(); i++) {
      assertSame("INodeFiles didn't Match", origFile, origFile.getBlocks()[i].getINode());
    }
  }

  /** 
   * Gives the count of blocks for a given number of files
   * @param files Array of INode files
   * @return total count of blocks
   */
  private int getTotalBlocks(INodeFile[] files) {
    int nBlocks=0;
    for(int i=0; i < files.length; i++) {
       nBlocks += files[i].numBlocks();
    }
    return nBlocks;
  }
  
  /** 
   * Creates the required number of files with one block each
   * @param nCount Number of INodes to create
   * @return Array of INode files
   */
  private INodeFile[] createINodeFiles(int nCount, String fileNamePrefix) {
    if(nCount <= 0)
      return new INodeFile[1];

    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile[] iNodes = new INodeFile[nCount];
    for (int i = 0; i < nCount; i++) {
      PermissionStatus perms = new PermissionStatus(userName, null,
          FsPermission.getDefault());
      iNodes[i] = new INodeFile(perms, null, replication, 0L, 0L,
          preferredBlockSize);
      iNodes[i].setLocalName(fileNamePrefix +  Integer.toString(i));
      BlockInfo newblock = new BlockInfo(replication);
      iNodes[i].addBlock(newblock);
    }
    
    return iNodes;
  }
}
