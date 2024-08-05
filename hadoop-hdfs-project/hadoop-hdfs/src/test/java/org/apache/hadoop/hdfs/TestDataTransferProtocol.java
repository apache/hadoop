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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
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
  
  private static final Logger LOG = LoggerFactory.getLogger(
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
      sock.connect(dnAddr, HdfsConstants.READ_TIMEOUT);
      sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);

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
        throw new IOException("Did not receive IOException when an exception " +
                              "is expected while reading from " + datanode); 
      }
      assertEquals(expected, received);
    } finally {
      IOUtils.closeSocket(sock);
    }
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
    new PipelineAck(100, new int[] {PipelineAck.combineHeader
      (PipelineAck.ECN.DISABLED, Status.SUCCESS)}).write
      (recvOut);
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
      datanode = InternalDataNodeTestUtils.getDNRegistrationForBP(
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
    
      DFSTestUtil.createFile(fileSys, file, fileLen, fileLen,
          fileSys.getDefaultBlockSize(file),
          fileSys.getDefaultReplication(file), 0L);

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
    new PipelineAck(100, new int[] {PipelineAck.combineHeader
      (PipelineAck.ECN.DISABLED, Status.ERROR)}).write(recvOut);
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
    new PipelineAck(100, new int[] {PipelineAck.combineHeader
      (PipelineAck.ECN.DISABLED, Status.SUCCESS)}).write(recvOut);
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

  @Test
  public void TestPipeLineAckCompatibility() throws IOException {
    DataTransferProtos.PipelineAckProto proto = DataTransferProtos
        .PipelineAckProto.newBuilder()
        .setSeqno(0)
        .addReply(Status.CHECKSUM_OK)
        .build();

    DataTransferProtos.PipelineAckProto newProto = DataTransferProtos
        .PipelineAckProto.newBuilder().mergeFrom(proto)
        .addFlag(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED,
                                           Status.CHECKSUM_OK))
        .build();

    ByteArrayOutputStream oldAckBytes = new ByteArrayOutputStream();
    proto.writeDelimitedTo(oldAckBytes);
    PipelineAck oldAck = new PipelineAck();
    oldAck.readFields(new ByteArrayInputStream(oldAckBytes.toByteArray()));
    assertEquals(PipelineAck.combineHeader(PipelineAck.ECN.DISABLED, Status
        .CHECKSUM_OK), oldAck.getHeaderFlag(0));

    PipelineAck newAck = new PipelineAck();
    ByteArrayOutputStream newAckBytes = new ByteArrayOutputStream();
    newProto.writeDelimitedTo(newAckBytes);
    newAck.readFields(new ByteArrayInputStream(newAckBytes.toByteArray()));
    assertEquals(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED, Status
        .CHECKSUM_OK), newAck.getHeaderFlag(0));
  }

  @Test
  public void testPipeLineAckCompatibilityWithSLOW() throws IOException {
    DataTransferProtos.PipelineAckProto proto = DataTransferProtos
        .PipelineAckProto.newBuilder()
        .setSeqno(0)
        .addReply(Status.CHECKSUM_OK)
        .addFlag(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED,
            Status.CHECKSUM_OK))
        .build();

    DataTransferProtos.PipelineAckProto newProto = DataTransferProtos
        .PipelineAckProto.newBuilder()
        .setSeqno(0)
        .addReply(Status.CHECKSUM_OK)
        .addFlag(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED,
            Status.CHECKSUM_OK, PipelineAck.SLOW.SLOW))
        .build();

    ByteArrayOutputStream oldAckBytes = new ByteArrayOutputStream();
    proto.writeDelimitedTo(oldAckBytes);
    PipelineAck oldAck = new PipelineAck();
    oldAck.readFields(new ByteArrayInputStream(oldAckBytes.toByteArray()));
    assertEquals(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED, Status
        .CHECKSUM_OK, PipelineAck.SLOW.DISABLED), oldAck.getHeaderFlag(0));

    PipelineAck newAck = new PipelineAck();
    ByteArrayOutputStream newAckBytes = new ByteArrayOutputStream();
    newProto.writeDelimitedTo(newAckBytes);
    newAck.readFields(new ByteArrayInputStream(newAckBytes.toByteArray()));
    assertEquals(PipelineAck.combineHeader(PipelineAck.ECN.SUPPORTED, Status
        .CHECKSUM_OK, PipelineAck.SLOW.SLOW), newAck.getHeaderFlag(0));
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
        checksum, CachingStrategy.newDefaultStrategy(), false, false,
        null, null, new String[0]);
  }

  @Test(timeout = 30000)
  public void testReleaseVolumeRefIfExceptionThrown()
      throws IOException, InterruptedException {
    Path file = new Path("dataprotocol.dat");
    int numDataNodes = 1;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        numDataNodes).build();
    try {
      cluster.waitActive();
      datanode = cluster.getFileSystem().getDataNodeStats(
          DatanodeReportType.LIVE)[0];
      dnAddr = NetUtils.createSocketAddr(datanode.getXferAddr());
      FileSystem fileSys = cluster.getFileSystem();

      int fileLen = Math.min(
          conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096), 4096);

      DFSTestUtil.createFile(fileSys, file, fileLen, fileLen,
          fileSys.getDefaultBlockSize(file),
          fileSys.getDefaultReplication(file), 0L);

      // Get the first blockid for the file.
      final ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);

      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock blk = new ExtendedBlock(bpid, firstBlock.getLocalBlock());
      sendBuf.reset();
      recvBuf.reset();

      // Delete the meta file to create a exception in BlockSender constructor.
      DataNode dn = cluster.getDataNodes().get(0);
      cluster.getMaterializedReplica(0, blk).deleteMeta();

      FsVolumeImpl volume = (FsVolumeImpl) DataNodeTestUtils.getFSDataset(
          dn).getVolume(blk);
      int beforeCnt = volume.getReferenceCount();

      sender.copyBlock(blk, BlockTokenSecretManager.DUMMY_TOKEN);
      sendRecvData("Copy a block.", false);
      Thread.sleep(3000);

      int afterCnt = volume.getReferenceCount();
      assertEquals(beforeCnt, afterCnt);

    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 90000)
  public void testCopyBlockCrossNamespace()
      throws IOException, InterruptedException, TimeoutException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2)).build();
    try {
      cluster.waitActive();
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();

      // Create one file with one block with one replica in Namespace0.
      Path ns0Path = new Path("/testCopyBlockCrossNamespace_0.txt");
      DistributedFileSystem ns0FS = cluster.getFileSystem(0);
      DFSTestUtil.createFile(ns0FS, ns0Path, 1024, (short) 2, 0);
      DFSTestUtil.waitReplication(ns0FS, ns0Path, (short) 2);
      HdfsFileStatus ns0FileStatus = (HdfsFileStatus) ns0FS.getFileStatus(ns0Path);
      LocatedBlocks locatedBlocks =
          ns0FS.getClient().getLocatedBlocks(ns0Path.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(1, locatedBlocks.getLocatedBlocks().size());
      assertTrue(locatedBlocks.isLastBlockComplete());
      LocatedBlock locatedBlockNS0 = locatedBlocks.get(0);

      DatanodeInfoWithStorage[] datanodeInfoWithStoragesNS0 = locatedBlockNS0.getLocations();
      assertEquals(2, datanodeInfoWithStoragesNS0.length);

      String ns0BlockLocation1 = datanodeInfoWithStoragesNS0[0].getHostName() + ":"
          + datanodeInfoWithStoragesNS0[0].getXferPort();
      String ns0BlockLocation2 = datanodeInfoWithStoragesNS0[1].getHostName() + ":"
          + datanodeInfoWithStoragesNS0[1].getXferPort();

      String[] favoredNodes = new String[2];

      favoredNodes[0] = ns0BlockLocation1;
      for (DataNode dn : dataNodes) {
        String dnInfo = dn.getDatanodeHostname() + ":" + dn.getXferPort();
        if (!dnInfo.equals(ns0BlockLocation1) && !dnInfo.equals(ns0BlockLocation2)) {
          favoredNodes[1] = dnInfo;
        }
      }

      // Create one similar file with two replicas in Namespace1.
      Path ns1Path = new Path("/testCopyBlockCrossNamespace_1.txt");
      DistributedFileSystem ns1FS = cluster.getFileSystem(1);
      FSDataOutputStream stream =
          ns1FS.create(ns1Path, ns0FileStatus.getPermission(), EnumSet.of(CreateFlag.CREATE),
              ns1FS.getClient().getConf().getIoBufferSize(), ns0FileStatus.getReplication(),
              ns0FileStatus.getBlockSize(), null, null, null, null, null);
      DFSOutputStream outputStream = (DFSOutputStream) stream.getWrappedStream();

      LocatedBlock locatedBlockNS1 =
          DFSOutputStream.addBlock(null, outputStream.getDfsClient(), ns1Path.getName(), null,
              outputStream.getFileId(), favoredNodes, null);
      assertEquals(2, locatedBlockNS1.getLocations().length);

      // Align the datanode.
      DatanodeInfoWithStorage[] datanodeInfoWithStoragesNS1 = locatedBlockNS1.getLocations();
      DatanodeInfoWithStorage sameDN = datanodeInfoWithStoragesNS0[0].getXferPort()
          == datanodeInfoWithStoragesNS1[0].getXferPort() ?
          datanodeInfoWithStoragesNS1[0] :
          datanodeInfoWithStoragesNS1[1];
      DatanodeInfoWithStorage differentDN = datanodeInfoWithStoragesNS0[0].getXferPort()
          == datanodeInfoWithStoragesNS1[0].getXferPort() ?
          datanodeInfoWithStoragesNS1[1] :
          datanodeInfoWithStoragesNS1[0];

      // HardLink locatedBlockNS0 to locatedBlockNS1 on same datanode.
      outputStream.copyBlockCrossNamespace(locatedBlockNS0.getBlock(),
          locatedBlockNS0.getBlockToken(), datanodeInfoWithStoragesNS0[0],
          locatedBlockNS1.getBlock(), locatedBlockNS1.getBlockToken(), sameDN);

      // Test when transfer throw exception client can know it.
      DataNodeFaultInjector.set(new DataNodeFaultInjector() {
        public void transferThrowException() throws IOException {
          throw new IOException("Transfer failed for fastcopy.");
        }
      });
      boolean transferError = false;
      try {
        outputStream.copyBlockCrossNamespace(locatedBlockNS0.getBlock(),
            locatedBlockNS0.getBlockToken(), datanodeInfoWithStoragesNS0[1],
            locatedBlockNS1.getBlock(), locatedBlockNS1.getBlockToken(), differentDN);
      } catch (IOException e) {
        transferError = true;
      }
      assertTrue(transferError);

      DataNodeFaultInjector.set(new DataNodeFaultInjector());
      // Transfer locatedBlockNS0 to locatedBlockNS1 on different datanode.
      outputStream.copyBlockCrossNamespace(locatedBlockNS0.getBlock(),
          locatedBlockNS0.getBlockToken(), datanodeInfoWithStoragesNS0[1],
          locatedBlockNS1.getBlock(), locatedBlockNS1.getBlockToken(), differentDN);

      // Check Lease Holder.
      RemoteIterator<OpenFileEntry> iterator = ns1FS.listOpenFiles();
      OpenFileEntry fileEntry = iterator.next();
      assertEquals(ns1Path.toUri().toString(), fileEntry.getFilePath());
      assertEquals(outputStream.getDfsClient().getClientName(), fileEntry.getClientName());

      outputStream.setUserAssignmentLastBlock(locatedBlockNS1.getBlock());
      stream.close();

      // Check Lease release.
      iterator = ns1FS.listOpenFiles();
      assertFalse(iterator.hasNext());

      long heartbeatInterval =
          conf.getTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY, DFS_HEARTBEAT_INTERVAL_DEFAULT,
              TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      Thread.sleep(heartbeatInterval * 2);

      // Do verification that the file in namespace1 should contain one block with two replicas.
      LocatedBlocks locatedBlocksNS1 = ns1FS.getClient().getNamenode()
          .getBlockLocations(ns1Path.toUri().getPath(), 0, Long.MAX_VALUE);
      assertEquals(1, locatedBlocksNS1.getLocatedBlocks().size());
      assertEquals(2, locatedBlocksNS1.getLocatedBlocks().get(0).getLocations().length);
      assertTrue(locatedBlocksNS1.isLastBlockComplete());

      for (DataNode dataNode : dataNodes) {
        if (dataNode.getXferPort() == datanodeInfoWithStoragesNS0[0].getXferPort()) {
          assertEquals(1L, dataNode.getMetrics().getBlocksReplicatedViaHardlink());
        } else if (dataNode.getXferPort() == datanodeInfoWithStoragesNS0[1].getXferPort()) {
          assertEquals(1L, dataNode.getMetrics().getBlocksReplicated());
        }
      }

    } finally {
      cluster.shutdown();
    }
  }
}
