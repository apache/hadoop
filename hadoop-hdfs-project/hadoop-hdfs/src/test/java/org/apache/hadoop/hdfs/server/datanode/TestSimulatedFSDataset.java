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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Before;
import org.junit.Test;

/**
 * this class tests the methods of the  SimulatedFSDataset.
 */
public class TestSimulatedFSDataset {
  Configuration conf = null;
  static final String bpid = "BP-TEST";
  static final int NUMBLOCKS = 20;
  static final int BLOCK_LENGTH_MULTIPLIER = 79;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
  }
  
  long blockIdToLen(long blkid) {
    return blkid*BLOCK_LENGTH_MULTIPLIER;
  }
  
  int addSomeBlocks(SimulatedFSDataset fsdataset, int startingBlockId)
      throws IOException {
    int bytesAdded = 0;
    for (int i = startingBlockId; i < startingBlockId+NUMBLOCKS; ++i) {
      ExtendedBlock b = new ExtendedBlock(bpid, i, 0, 0); 
      // we pass expected len as zero, - fsdataset should use the sizeof actual
      // data written
      ReplicaInPipelineInterface bInfo = fsdataset.createRbw(
          StorageType.DEFAULT, b, false).getReplica();
      ReplicaOutputStreams out = bInfo.createStreams(true,
          DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512));
      try {
        OutputStream dataOut  = out.getDataOut();
        assertEquals(0, fsdataset.getLength(b));
        for (int j=1; j <= blockIdToLen(i); ++j) {
          dataOut.write(j);
          assertEquals(j, bInfo.getBytesOnDisk()); // correct length even as we write
          bytesAdded++;
        }
      } finally {
        out.close();
      }
      b.setNumBytes(blockIdToLen(i));
      fsdataset.finalizeBlock(b, false);
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
    }
    return bytesAdded;  
  }
  int addSomeBlocks(SimulatedFSDataset fsdataset ) throws IOException {
    return addSomeBlocks(fsdataset, 1);
  }
  
  @Test
  public void testFSDatasetFactory() {
    final Configuration conf = new Configuration();
    FsDatasetSpi.Factory<?> f = FsDatasetSpi.Factory.getFactory(conf);
    assertEquals(FsDatasetFactory.class, f.getClass());
    assertFalse(f.isSimulated());

    SimulatedFSDataset.setFactory(conf);
    FsDatasetSpi.Factory<?> s = FsDatasetSpi.Factory.getFactory(conf);
    assertEquals(SimulatedFSDataset.Factory.class, s.getClass());
    assertTrue(s.isSimulated());
  }

  @Test
  public void testGetMetaData() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, 1, 5, 0);
    try {
      assertTrue(fsdataset.getMetaDataInputStream(b) == null);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    addSomeBlocks(fsdataset); // Only need to add one but ....
    b = new ExtendedBlock(bpid, 1, 0, 0);
    InputStream metaInput = fsdataset.getMetaDataInputStream(b);
    DataInputStream metaDataInput = new DataInputStream(metaInput);
    short version = metaDataInput.readShort();
    assertEquals(BlockMetadataHeader.VERSION, version);
    DataChecksum checksum = DataChecksum.newDataChecksum(metaDataInput);
    assertEquals(DataChecksum.Type.NULL, checksum.getChecksumType());
    assertEquals(0, checksum.getChecksumSize());  
  }


  @Test
  public void testStorageUsage() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    assertEquals(fsdataset.getDfsUsed(), 0);
    assertEquals(fsdataset.getRemaining(), fsdataset.getCapacity());
    int bytesAdded = addSomeBlocks(fsdataset);
    assertEquals(bytesAdded, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded,  fsdataset.getRemaining());
  }



  void checkBlockDataAndSize(SimulatedFSDataset fsdataset, ExtendedBlock b,
      long expectedLen) throws IOException { 
    InputStream input = fsdataset.getBlockInputStream(b);
    long lengthRead = 0;
    int data;
    while ((data = input.read()) != -1) {
      assertEquals(SimulatedFSDataset.DEFAULT_DATABYTE, data);
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);
  }
  
  @Test
  public void testWriteRead() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    addSomeBlocks(fsdataset);
    for (int i=1; i <= NUMBLOCKS; ++i) {
      ExtendedBlock b = new ExtendedBlock(bpid, i, 0, 0);
      assertTrue(fsdataset.isValidBlock(b));
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
      checkBlockDataAndSize(fsdataset, b, blockIdToLen(i));
    }
  }

  @Test
  public void testGetBlockReport() throws IOException {
    SimulatedFSDataset fsdataset = getSimulatedFSDataset(); 
    BlockListAsLongs blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(0, blockReport.getNumberOfBlocks());
    addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
  }
  
  @Test
  public void testInjectionEmpty() throws IOException {
    SimulatedFSDataset fsdataset = getSimulatedFSDataset(); 
    BlockListAsLongs blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(0, blockReport.getNumberOfBlocks());
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    
    // Inject blocks into an empty fsdataset
    //  - injecting the blocks we got above.
    SimulatedFSDataset sfsdataset = getSimulatedFSDataset();
    sfsdataset.injectBlocks(bpid, blockReport);
    blockReport = sfsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset
          .getLength(new ExtendedBlock(bpid, b)));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded, sfsdataset.getRemaining());
  }

  @Test
  public void testInjectionNonEmpty() throws IOException {
    SimulatedFSDataset fsdataset = getSimulatedFSDataset(); 
    BlockListAsLongs blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(0, blockReport.getNumberOfBlocks());
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    fsdataset = null;
    
    // Inject blocks into an non-empty fsdataset
    //  - injecting the blocks we got above.
    SimulatedFSDataset sfsdataset = getSimulatedFSDataset();
    // Add come blocks whose block ids do not conflict with
    // the ones we are going to inject.
    bytesAdded += addSomeBlocks(sfsdataset, NUMBLOCKS+1);
    sfsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    sfsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    sfsdataset.injectBlocks(bpid, blockReport);
    blockReport = sfsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS*2, blockReport.getNumberOfBlocks());
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset
          .getLength(new ExtendedBlock(bpid, b)));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded,  sfsdataset.getRemaining());
    
    // Now test that the dataset cannot be created if it does not have sufficient cap
    conf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY, 10);
 
    try {
      sfsdataset = getSimulatedFSDataset();
      sfsdataset.addBlockPool(bpid, conf);
      sfsdataset.injectBlocks(bpid, blockReport);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
  }

  public void checkInvalidBlock(ExtendedBlock b) {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    assertFalse(fsdataset.isValidBlock(b));
    try {
      fsdataset.getLength(b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.getBlockInputStream(b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.finalizeBlock(b, false);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
  }
  
  @Test
  public void testInValidBlocks() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, 1, 5, 0);
    checkInvalidBlock(b);
    
    // Now check invlaid after adding some blocks
    addSomeBlocks(fsdataset);
    b = new ExtendedBlock(bpid, NUMBLOCKS + 99, 5, 0);
    checkInvalidBlock(b);
  }

  @Test
  public void testInvalidate() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    int bytesAdded = addSomeBlocks(fsdataset);
    Block[] deleteBlocks = new Block[2];
    deleteBlocks[0] = new Block(1, 0, 0);
    deleteBlocks[1] = new Block(2, 0, 0);
    fsdataset.invalidate(bpid, deleteBlocks);
    checkInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[0]));
    checkInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[1]));
    long sizeDeleted = blockIdToLen(1) + blockIdToLen(2);
    assertEquals(bytesAdded-sizeDeleted, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded+sizeDeleted,  fsdataset.getRemaining());
    
    // Now make sure the rest of the blocks are valid
    for (int i=3; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(new ExtendedBlock(bpid, b)));
    }
  }
  
  private SimulatedFSDataset getSimulatedFSDataset() {
    SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
    fsdataset.addBlockPool(bpid, conf);
    return fsdataset;
  }
}
