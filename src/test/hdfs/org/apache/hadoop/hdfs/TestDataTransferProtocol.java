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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Op.READ_BLOCK;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Op.WRITE_BLOCK;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 */
public class TestDataTransferProtocol extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(
                    "org.apache.hadoop.hdfs.TestDataTransferProtocol");
  
  DatanodeID datanode;
  InetSocketAddress dnAddr;
  ByteArrayOutputStream sendBuf = new ByteArrayOutputStream(128);
  DataOutputStream sendOut = new DataOutputStream(sendBuf);
  ByteArrayOutputStream recvBuf = new ByteArrayOutputStream(128);
  DataOutputStream recvOut = new DataOutputStream(recvBuf);

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
      for (int i=0; i<retBuf.length; i++) {
        System.out.print(retBuf[i]);
      }
      System.out.println(":");
      
      if (eofExpected) {
        throw new IOException("Did not recieve IOException when an exception " +
                              "is expected while reading from " + 
                              datanode.getName());
      }
      
      byte[] needed = recvBuf.toByteArray();
      for (int i=0; i<retBuf.length; i++) {
        System.out.print(retBuf[i]);
        assertEquals("checking byte[" + i + "]", needed[i], retBuf[i]);
      }
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
  
  private void writeZeroLengthPacket(Block block, String description)
  throws IOException {
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    sendOut.writeInt(512);         // checksum size
    sendOut.writeInt(8);           // size of packet
    sendOut.writeLong(block.getNumBytes());          // OffsetInBlock
    sendOut.writeLong(100);        // sequencenumber
    sendOut.writeBoolean(true);    // lastPacketInBlock

    sendOut.writeInt(0);           // chunk length
    sendOut.writeInt(0);           // zero checksum
        
    //ok finally write a block with 0 len
    SUCCESS.write(recvOut);
    Text.writeString(recvOut, "");
    new PipelineAck(100, new Status[]{SUCCESS}).write(recvOut);
    sendRecvData(description, false);
  }
  
  private void testWrite(Block block, BlockConstructionStage stage, long newGS,
      String description, Boolean eofExcepted) throws IOException {
    sendBuf.reset();
    recvBuf.reset();
    DataTransferProtocol.Sender.opWriteBlock(sendOut, 
        block.getBlockId(), block.getGenerationStamp(), 0,
        stage, newGS, block.getNumBytes(), block.getNumBytes(), "cl", null,
        new DatanodeInfo[1], BlockAccessToken.DUMMY_TOKEN);
    if (eofExcepted) {
      ERROR.write(recvOut);
      sendRecvData(description, true);
    } else if (stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
      //ok finally write a block with 0 len
      SUCCESS.write(recvOut);
      Text.writeString(recvOut, ""); // first bad node
      sendRecvData(description, false);
    } else {
      writeZeroLengthPacket(block, description);
    }
  }
  
  @Test public void testOpWrite() throws IOException {
    int numDataNodes = 1;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    try {
      cluster.waitActive();
      datanode = cluster.getDataNodes().get(0).dnRegistration;
      dnAddr = NetUtils.createSocketAddr(datanode.getName());
      FileSystem fileSys = cluster.getFileSystem();

      /* Test writing to finalized replicas */
      Path file = new Path("dataprotocol.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      // get the first blockid for the file
      Block firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
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

      /* Test writing to a new block */
      long newBlockId = firstBlock.getBlockId() + 1;
      Block newBlock = new Block(newBlockId, 0, 
          firstBlock.getGenerationStamp());

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
        newGS = newBlock.getGenerationStamp() + 1;
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
  
@Test  public void testDataTransferProtocol() throws IOException {
    Random random = new Random();
    int oneMil = 1024*1024;
    Path file = new Path("dataprotocol.dat");
    int numDataNodes = 1;
    
    Configuration conf = new HdfsConfiguration();
    conf.setInt("dfs.replication", numDataNodes); 
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    try {
    cluster.waitActive();
    DFSClient dfsClient = new DFSClient(
                 new InetSocketAddress("localhost", cluster.getNameNodePort()),
                 conf);                
    datanode = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    dnAddr = NetUtils.createSocketAddr(datanode.getName());
    FileSystem fileSys = cluster.getFileSystem();
    
    int fileLen = Math.min(conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096), 4096);
    
    createFile(fileSys, file, fileLen);

    // get the first blockid for the file
    Block firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);
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
    sendOut.writeByte(WRITE_BLOCK.code - 1);
    sendRecvData("Wrong Op Code", true);
    
    /* Test OP_WRITE_BLOCK */
    sendBuf.reset();
    DataTransferProtocol.Sender.opWriteBlock(sendOut, 
        newBlockId, 0L, 0,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L, 0L, 0L, "cl", null,
        new DatanodeInfo[1], BlockAccessToken.DUMMY_TOKEN);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    
    // bad bytes per checksum
    sendOut.writeInt(-1-random.nextInt(oneMil));
    recvBuf.reset();
    ERROR.write(recvOut);
    sendRecvData("wrong bytesPerChecksum while writing", true);

    sendBuf.reset();
    recvBuf.reset();
    DataTransferProtocol.Sender.opWriteBlock(sendOut,
        ++newBlockId, 0L, 0,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L, 0L, 0L, "cl", null,
        new DatanodeInfo[1], BlockAccessToken.DUMMY_TOKEN);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    sendOut.writeInt(512);
    sendOut.writeInt(4);           // size of packet
    sendOut.writeLong(0);          // OffsetInBlock
    sendOut.writeLong(100);        // sequencenumber
    sendOut.writeBoolean(false);   // lastPacketInBlock
    
    // bad data chunk length
    sendOut.writeInt(-1-random.nextInt(oneMil));
    SUCCESS.write(recvOut);
    Text.writeString(recvOut, "");
    new PipelineAck(100, new Status[]{ERROR}).write(recvOut);
    sendRecvData("negative DATA_CHUNK len while writing block " + newBlockId, 
                 true);

    // test for writing a valid zero size block
    sendBuf.reset();
    recvBuf.reset();
    DataTransferProtocol.Sender.opWriteBlock(sendOut, 
        ++newBlockId, 0L, 0,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 0L, 0L, 0L, "cl", null,
        new DatanodeInfo[1], BlockAccessToken.DUMMY_TOKEN);
    sendOut.writeByte((byte)DataChecksum.CHECKSUM_CRC32);
    sendOut.writeInt(512);         // checksum size
    sendOut.writeInt(8);           // size of packet
    sendOut.writeLong(0);          // OffsetInBlock
    sendOut.writeLong(100);        // sequencenumber
    sendOut.writeBoolean(true);    // lastPacketInBlock

    sendOut.writeInt(0);           // chunk length
    sendOut.writeInt(0);           // zero checksum
    sendOut.flush();
    //ok finally write a block with 0 len
    SUCCESS.write(recvOut);
    Text.writeString(recvOut, "");
    new PipelineAck(100, new Status[]{SUCCESS}).write(recvOut);
    sendRecvData("Writing a zero len block blockid " + newBlockId, false);
    
    /* Test OP_READ_BLOCK */

    // bad block id
    sendBuf.reset();
    recvBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    newBlockId = firstBlock.getBlockId()-1;
    sendOut.writeLong(newBlockId);
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0L);
    sendOut.writeLong(fileLen);
    ERROR.write(recvOut);
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong block ID " + newBlockId + " for read", false); 

    // negative block start offset
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(-1L);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Negative start-offset for read for block " + 
                 firstBlock.getBlockId(), false);

    // bad block start offset
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(fileLen);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong start-offset for reading block " +
                 firstBlock.getBlockId(), false);
    
    // negative length is ok. Datanode assumes we want to read the whole block.
    recvBuf.reset();
    SUCCESS.write(recvOut);    
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(-1-random.nextInt(oneMil));
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Negative length for reading block " +
                 firstBlock.getBlockId(), false);
    
    // length is more than size of block.
    recvBuf.reset();
    ERROR.write(recvOut);    
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(fileLen + 1);
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    sendRecvData("Wrong length for reading block " +
                 firstBlock.getBlockId(), false);
    
    //At the end of all this, read the file to make sure that succeeds finally.
    sendBuf.reset();
    sendOut.writeShort((short)DataTransferProtocol.DATA_TRANSFER_VERSION);
    READ_BLOCK.write(sendOut);
    sendOut.writeLong(firstBlock.getBlockId());
    sendOut.writeLong(firstBlock.getGenerationStamp());
    sendOut.writeLong(0);
    sendOut.writeLong(fileLen);
    Text.writeString(sendOut, "cl");
    BlockAccessToken.DUMMY_TOKEN.write(sendOut);
    readFile(fileSys, file, fileLen);
    } finally {
      cluster.shutdown();
    }
  }
}
