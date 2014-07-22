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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 */
public class TestDataTransferProtocol {
  
  private static final Log LOG = LogFactory.getLog(
                    "org.apache.hadoop.hdfs.TestDataTransferProtocol");

  private static final DataChecksum DEFAULT_CHECKSUM =
    DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);
  
  DatanodeID datanode;
  InetSocketAddress dnAddr;
  final ByteArrayOutputStream sendBuf = new ByteArrayOutputStream(128);
  final DataOutputStream sendOut = new DataOutputStream(sendBuf);
  final Sender sender = new Sender(sendOut);
  final ByteArrayOutputStream recvBuf = new ByteArrayOutputStream(128);
  final DataOutputStream recvOut = new DataOutputStream(recvBuf);

  private void sendRecvData(String testDescription,
                            boolean eofExpected) throws IOException {
    /* Opens a socket to datanode
     * sends the data in sendBuf.
     * If there is data in expectedBuf, expects to receive the data
     *     from datanode that matches expectedBuf.
     * If there is an exception while recieving, throws it
     *     only if exceptionExcepted is false.
     */
    
    Socket sock = null;
    try {
      
      if ( testDescription != null ) {
        LOG.info("Testing : " + testDescription);
      }
      LOG.info("Going to write:" +
          StringUtils.byteToHexString(sendBuf.toByteArray()));
      
      sock = new Socket();
      sock.connect(dnAddr, HdfsServerConstants.READ_TIMEOUT);
      sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
      
      OutputStream out = sock.getOutputStream();
      // Should we excuse 
      byte[] retBuf = new byte[recvBuf.size()];
      
      DataInputStream in = new DataInputStream(sock.getInputStream());
      out.write(sendBuf.toByteArray());
      out.flush();
      try {
        in.readFully(retBuf);
      } catch (EOFException eof) {
        if ( eofExpected ) {
          LOG.info("Got EOF as expected.");
          return;
        }
        throw eof;
      }

      String received = StringUtils.byteToHexString(retBuf);
      String expected = StringUtils.byteToHexString(recvBuf.toByteArray());
      LOG.info("Received: " + received);
      LOG.info("Expected: " + expected);
      
      if (eofExpected) {
        throw new IOException("Did not recieve IOException when an exception " +
                              "is expected while reading from " + datanode); 
      }
      assertEquals(expected, received);
    } finally {
      IOUtils.closeSocket(sock);
    }
  }
  
  void createFile(FileSystem fs, Path path, int fileLen) throws IOException {
    byte [] arr = new byte[fileLen];
    FSDataOutputStream out = fs.create(path);
    out.write(arr);
    out.close();
  }
  
  void readFile(FileSystem fs, Path path, int fileLen) throws IOException {
    byte [] arr = new byte[fileLen];
    FSDataInputStream in = fs.open(path);
    in.readFully(arr);
  }
  
  private void writeZeroLengthPacket(ExtendedBlock block, String description)
  throws IOException {
    PacketHeader hdr = new PacketHeader(
      8,                   // size of packet
      block.getNumBytes(), // OffsetInBlock
      100,                 // sequencenumber
      true,                // lastPacketInBlock
      0,                   // chunk length
      false);               // sync block
    hdr.write(sendOut);
    sendOut.writeInt(0);           // zero checksum

    //ok finally write a block with 0 len
    sendResponse(Status.SUCCESS, "", null, recvOut);
    new PipelineAck(100, new Status[]{Status.SUCCESS}).write(recvOut);
    sendRecvData(description, false);
  }
  
  private void sendResponse(Status status, String firstBadLink,
      String message,
      DataOutputStream out)
  throws IOException {
    Builder builder = BlockOpResponseProto.newBuilder().setStatus(status);
    if (firstBadLink != null) {
      builder.setFirstBadLink(firstBadLink);
    }
    if (message != null) {
      builder.setMessage(message);
    }
    builder.build()
      .writeDelimitedTo(out);
  }

  private void testWrite(ExtendedBlock block, BlockConstructionStage stage, long newGS,
      String description, Boolean eofExcepted) throws IOException {
    sendBuf.reset();
    recvBuf.reset();
    writeBlock(block, stage, newGS, DEFAULT_CHECKSUM);
    if (eofExcepted) {
      sendResponse(Status.ERROR, null, null, recvOut);
      sendRecvData(description, true);
    } else if (stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
      //ok finally write a block with 0 len
      sendResponse(Status.SUCCESS, "", null, recvOut);
      sendRecvData(description, false);
    } else {
      writeZeroLengthPacket(block, description);
    }
  }
  
  @Test 
  public void testOpWrite() throws IOException {
    int numDataNodes = 1;
    final long BLOCK_ID_FUDGE = 128;
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    try {
      cluster.waitActive();
      String poolId = cluster.getNamesystem().getBlockPoolId(); 
      datanode = DataNodeTestUtils.getDNRegistrationForBP(
          cluster.getDataNodes().get(0), poolId);
      dnAddr = NetUtils.createSocketAddr(datanode.getXferAddr());
      FileSystem fileSys = cluster.getFileSystem();

      /* Test writing to finalized replicas */
      Path file = new Path("dataprotocol.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      // get the first blockid for the file
      ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
      // test PIPELINE_SETUP_CREATE on a finalized block
      testWrite(firstBlock, BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L,
          "Cannot create an existing block", true);
      // test PIPELINE_DATA_STREAMING on a finalized block
      testWrite(firstBlock, BlockConstructionStage.DATA_STREAMING, 0L,
          "Unexpected stage", true);
      // test PIPELINE_SETUP_STREAMING_RECOVERY on an existing block
      long newGS = firstBlock.getGenerationStamp() + 1;
      testWrite(firstBlock, 
          BlockConstructionStage.PIPELINE_SETUP_STREAMING_RECOVERY, 
          newGS, "Cannot recover data streaming to a finalized replica", true);
      // test PIPELINE_SETUP_APPEND on an existing block
      newGS = firstBlock.getGenerationStamp() + 1;
      testWrite(firstBlock, 
          BlockConstructionStage.PIPELINE_SETUP_APPEND,
          newGS, "Append to a finalized replica", false);
      firstBlock.setGenerationStamp(newGS);
      // test PIPELINE_SETUP_APPEND_RECOVERY on an existing block
      file = new Path("dataprotocol1.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
      newGS = firstBlock.getGenerationStamp() + 1;
      testWrite(firstBlock, 
          BlockConstructionStage.PIPELINE_SETUP_APPEND_RECOVERY, newGS,
          "Recover appending to a finalized replica", false);
      // test PIPELINE_CLOSE_RECOVERY on an existing block
      file = new Path("dataprotocol2.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
      newGS = firstBlock.getGenerationStamp() + 1;
      testWrite(firstBlock, 
          BlockConstructionStage.PIPELINE_CLOSE_RECOVERY, newGS,
          "Recover failed close to a finalized replica", false);
      firstBlock.setGenerationStamp(newGS);

      // Test writing to a new block. Don't choose the next sequential
      // block ID to avoid conflicting with IDs chosen by the NN.
      long newBlockId = firstBlock.getBlockId() + BLOCK_ID_FUDGE;
      ExtendedBlock newBlock = new ExtendedBlock(firstBlock.getBlockPoolId(),
          newBlockId, 0, firstBlock.getGenerationStamp());

      // test PIPELINE_SETUP_CREATE on a new block
      testWrite(newBlock, BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L,
          "Create a new block", false);
      // test PIPELINE_SETUP_STREAMING_RECOVERY on a new block
      newGS = newBlock.getGenerationStamp() + 1;
      newBlock.setBlockId(newBlock.getBlockId()+1);
      testWrite(newBlock, 
          BlockConstructionStage.PIPELINE_SETUP_STREAMING_RECOVERY, newGS,
          "Recover a new block", true);
      
      // test PIPELINE_SETUP_APPEND on a new block
      newGS = newBlock.getGenerationStamp() + 1;
      testWrite(newBlock, 
          BlockConstructionStage.PIPELINE_SETUP_APPEND, newGS,
          "Cannot append to a new block", true);

      // test PIPELINE_SETUP_APPEND_RECOVERY on a new block
      newBlock.setBlockId(newBlock.getBlockId()+1);
      newGS = newBlock.getGenerationStamp() + 1;
      testWrite(newBlock, 
          BlockConstructionStage.PIPELINE_SETUP_APPEND_RECOVERY, newGS,
          "Cannot append to a new block", true);

      /* Test writing to RBW replicas */
      Path file1 = new Path("dataprotocol1.dat");    
      DFSTestUtil.createFile(fileSys, file1, 1L, (short)numDataNodes, 0L);
      DFSOutputStream out = (DFSOutputStream)(fileSys.append(file1).
          getWrappedStream());
      out.write(1);
      out.hflush();
      FSDataInputStream in = fileSys.open(file1);
      firstBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
      firstBlock.setNumBytes(2L);
      
      try {
        // test PIPELINE_SETUP_CREATE on a RBW block
        testWrite(firstBlock, BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L,
            "Cannot create a RBW block", true);
        // test PIPELINE_SETUP_APPEND on an existing block
        newGS = firstBlock.getGenerationStamp() + 1;
        testWrite(firstBlock, BlockConstructionStage.PIPELINE_SETUP_APPEND,
            newGS, "Cannot append to a RBW replica", true);
        // test PIPELINE_SETUP_APPEND on an existing block
        testWrite(firstBlock, 
            BlockConstructionStage.PIPELINE_SETUP_APPEND_RECOVERY,
            newGS, "Recover append to a RBW replica", false);
        firstBlock.setGenerationStamp(newGS);
        // test PIPELINE_SETUP_STREAMING_RECOVERY on a RBW block
        file = new Path("dataprotocol2.dat");    
        DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
        out = (DFSOutputStream)(fileSys.append(file).
            getWrappedStream()); 
        out.write(1);
        out.hflush();
        in = fileSys.open(file);
        firstBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
        firstBlock.setNumBytes(2L);
        newGS = firstBlock.getGenerationStamp() + 1;
        testWrite(firstBlock, 
            BlockConstructionStage.PIPELINE_SETUP_STREAMING_RECOVERY,
            newGS, "Recover a RBW replica", false);
      } finally {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
      }

    } finally {
      cluster.shutdown();
    }
  }
  
  @Test  
  public void testDataTransferProtocol() throws IOException {
    Random random = new Random();
    int oneMil = 1024*1024;
    Path file = new Path("dataprotocol.dat");
    int numDataNodes = 1;
    
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes); 
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    try {
    cluster.waitActive();
    datanode = cluster.getFileSystem().getDataNodeStats(DatanodeReportType.LIVE)[0];
    dnAddr = NetUtils.createSocketAddr(datanode.getXferAddr());
    FileSystem fileSys = cluster.getFileSystem();
    
    int fileLen = Math.min(conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096), 4096);
    
    createFile(fileSys, file, fileLen);

    // get the first blockid for the file
    final ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
    final String poolId = firstBlock.getBlockPoolId();
    long newBlockId = firstBlock.getBlockId() + 1;

    recvBuf.reset();
    sendBuf.reset();
    
    // bad version
    recvOut.writeShort((short)(DataTransferProtocol.DATA_TRANSFER_VERSION-1));
    sendOut.writeShort((short)(DataTransferProtocol.DATA_TRANSFER_VERSION-1));
    sendRecvData("Wrong Version", true);

    // bad ops
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    sendOut.writeByte(Op.WRITE_BLOCK.code - 1);
    sendRecvData("Wrong Op Code", true);
    
    /* Test OP_WRITE_BLOCK */
    sendBuf.reset();
    
    DataChecksum badChecksum = Mockito.spy(DEFAULT_CHECKSUM);
    Mockito.doReturn(-1).when(badChecksum).getBytesPerChecksum();

    writeBlock(poolId, newBlockId, badChecksum);
    recvBuf.reset();
    sendResponse(Status.ERROR, null, null, recvOut);
    sendRecvData("wrong bytesPerChecksum while writing", true);

    sendBuf.reset();
    recvBuf.reset();
    writeBlock(poolId, ++newBlockId, DEFAULT_CHECKSUM);

    PacketHeader hdr = new PacketHeader(
      4,     // size of packet
      0,     // offset in block,
      100,   // seqno
      false, // last packet
      -1 - random.nextInt(oneMil), // bad datalen
      false);
    hdr.write(sendOut);

    sendResponse(Status.SUCCESS, "", null, recvOut);
    new PipelineAck(100, new Status[]{Status.ERROR}).write(recvOut);
    sendRecvData("negative DATA_CHUNK len while writing block " + newBlockId, 
                 true);

    // test for writing a valid zero size block
    sendBuf.reset();
    recvBuf.reset();
    writeBlock(poolId, ++newBlockId, DEFAULT_CHECKSUM);

    hdr = new PacketHeader(
      8,     // size of packet
      0,     // OffsetInBlock
      100,   // sequencenumber
      true,  // lastPacketInBlock
      0,     // chunk length
      false);    
    hdr.write(sendOut);
    sendOut.writeInt(0);           // zero checksum
    sendOut.flush();
    //ok finally write a block with 0 len
    sendResponse(Status.SUCCESS, "", null, recvOut);
    new PipelineAck(100, new Status[]{Status.SUCCESS}).write(recvOut);
    sendRecvData("Writing a zero len block blockid " + newBlockId, false);
    
    /* Test OP_READ_BLOCK */

    String bpid = cluster.getNamesystem().getBlockPoolId();
    ExtendedBlock blk = new ExtendedBlock(bpid, firstBlock.getLocalBlock());
    long blkid = blk.getBlockId();
    // bad block id
    sendBuf.reset();
    recvBuf.reset();
    blk.setBlockId(blkid-1);
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        0L, fileLen, true, CachingStrategy.newDefaultStrategy());
    sendRecvData("Wrong block ID " + newBlockId + " for read", false); 

    // negative block start offset -1L
    sendBuf.reset();
    blk.setBlockId(blkid);
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        -1L, fileLen, true, CachingStrategy.newDefaultStrategy());
    sendRecvData("Negative start-offset for read for block " + 
                 firstBlock.getBlockId(), false);

    // bad block start offset
    sendBuf.reset();
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        fileLen, fileLen, true, CachingStrategy.newDefaultStrategy());
    sendRecvData("Wrong start-offset for reading block " +
                 firstBlock.getBlockId(), false);
    
    // negative length is ok. Datanode assumes we want to read the whole block.
    recvBuf.reset();
    
    BlockOpResponseProto.newBuilder()
      .setStatus(Status.SUCCESS)
      .setReadOpChecksumInfo(ReadOpChecksumInfoProto.newBuilder()
          .setChecksum(DataTransferProtoUtil.toProto(DEFAULT_CHECKSUM))
          .setChunkOffset(0L))
      .build()
      .writeDelimitedTo(recvOut);
    
    sendBuf.reset();
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        0L, -1L-random.nextInt(oneMil), true,
        CachingStrategy.newDefaultStrategy());
    sendRecvData("Negative length for reading block " +
                 firstBlock.getBlockId(), false);
    
    // length is more than size of block.
    recvBuf.reset();
    sendResponse(Status.ERROR, null,
        "opReadBlock " + firstBlock +
        " received exception java.io.IOException:  " +
        "Offset 0 and length 4097 don't match block " + firstBlock + " ( blockLen 4096 )",
        recvOut);
    sendBuf.reset();
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        0L, fileLen+1, true, CachingStrategy.newDefaultStrategy());
    sendRecvData("Wrong length for reading block " +
                 firstBlock.getBlockId(), false);
    
    //At the end of all this, read the file to make sure that succeeds finally.
    sendBuf.reset();
    sender.readBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        0L, fileLen, true, CachingStrategy.newDefaultStrategy());
    readFile(fileSys, file, fileLen);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testPacketHeader() throws IOException {
    PacketHeader hdr = new PacketHeader(
      4,                   // size of packet
      1024,                // OffsetInBlock
      100,                 // sequencenumber
      false,               // lastPacketInBlock
      4096,                // chunk length
      false);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    hdr.write(new DataOutputStream(baos));

    // Read back using DataInput
    PacketHeader readBack = new PacketHeader();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    readBack.readFields(new DataInputStream(bais));
    assertEquals(hdr, readBack);

    // Read back using ByteBuffer
    readBack = new PacketHeader();
    readBack.readFields(ByteBuffer.wrap(baos.toByteArray()));
    assertEquals(hdr, readBack);

    assertTrue(hdr.sanityCheck(99));
    assertFalse(hdr.sanityCheck(100));
  }

  void writeBlock(String poolId, long blockId, DataChecksum checksum) throws IOException {
    writeBlock(new ExtendedBlock(poolId, blockId),
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L, checksum);
  }

  void writeBlock(ExtendedBlock block, BlockConstructionStage stage,
      long newGS, DataChecksum checksum) throws IOException {
    sender.writeBlock(block, StorageType.DEFAULT,
        BlockTokenSecretManager.DUMMY_TOKEN, "cl",
        new DatanodeInfo[1], new StorageType[1], null, stage,
        0, block.getNumBytes(), block.getNumBytes(), newGS,
        checksum, CachingStrategy.newDefaultStrategy());
  }
}
