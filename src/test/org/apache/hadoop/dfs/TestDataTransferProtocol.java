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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.dfs.FSConstants.DatanodeReportType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 */
public class TestDataTransferProtocol extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(
                    "org.apache.hadoop.dfs.TestDataTransferProtocol");
  
  DatanodeID datanode;
  InetSocketAddress dnAddr;
  byte[] sendBuf = new byte[128];
  byte[] recvBuf = new byte[128];
  ByteBuffer byteBuf = ByteBuffer.wrap(sendBuf);
  ByteBuffer recvByteBuf = ByteBuffer.wrap(recvBuf);
  
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
      sock.connect(dnAddr, FSConstants.READ_TIMEOUT);
      sock.setSoTimeout(FSConstants.READ_TIMEOUT);
      
      OutputStream out = sock.getOutputStream();
      // Should we excuse 
      out.write(sendBuf, 0, byteBuf.position());
      byte[] retBuf = new byte[recvByteBuf.position()];
      
      DataInputStream in = new DataInputStream(sock.getInputStream());
      try {
        in.readFully(retBuf);
      } catch (EOFException eof) {
        if ( eofExpected ) {
          LOG.info("Got EOF as expected.");
          return;
        }
        throw eof;
      }
      
      if (eofExpected) {
        throw new IOException("Did not recieve IOException when an exception " +
                              "is expected while reading from " + 
                              datanode.getName());
      }
      
      for (int i=0; i<retBuf.length; i++) {
        assertEquals("checking byte[" + i + "]", recvBuf[i], retBuf[i]);
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
  
  Block getFirstBlock(FileSystem fs, Path path) throws IOException {
    DFSDataInputStream in = 
      (DFSDataInputStream) ((DistributedFileSystem)fs).open(path);
    in.readByte();
    return in.getCurrentBlock();
  }
  
  public void testDataTransferProtocol() throws IOException {
    Random random = new Random();
    int oneMil = 1024*1024;
    Path file = new Path("dataprotocol.dat");
    int numDataNodes = 1;
    
    Configuration conf = new Configuration();
    conf.setInt("dfs.replication", numDataNodes); 
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    cluster.waitActive();
    DFSClient dfsClient = new DFSClient(
                 new InetSocketAddress("localhost", cluster.getNameNodePort()),
                 conf);                
    datanode = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    dnAddr = DataNode.createSocketAddr(datanode.getName());
    FileSystem fileSys = cluster.getFileSystem();
    
    int fileLen = Math.min(conf.getInt("dfs.block.size", 4096), 4096);
    
    createFile(fileSys, file, fileLen);

    // get the first blockid for the file
    Block firstBlock = getFirstBlock(fileSys, file);
    long newBlockId = firstBlock.getBlockId() + 1;

    recvByteBuf.position(1);
    byteBuf.position(0);
    
    int versionPos = 0;
    byteBuf.putShort((short)(FSConstants.DATA_TRANFER_VERSION-1));
    sendRecvData("Wrong Version", true);
    // correct the version
    byteBuf.putShort(versionPos, (short)FSConstants.DATA_TRANFER_VERSION);
    
    int opPos = byteBuf.position();
    byteBuf.put((byte)(FSConstants.OP_WRITE_BLOCK-1));
    sendRecvData("Wrong Op Code", true);
    
    /* Test OP_WRITE_BLOCK */
    
    byteBuf.position(opPos);
    // Initially write correct values
    byteBuf.put((byte)FSConstants.OP_WRITE_BLOCK);
    int blockPos = byteBuf.position();
    byteBuf.putLong(newBlockId);
    int targetPos = byteBuf.position();
    byteBuf.putInt(0);
    int checksumPos = byteBuf.position();
    byteBuf.put((byte)DataChecksum.CHECKSUM_CRC32);
    
    byteBuf.putInt(-1-random.nextInt(oneMil));
    sendRecvData("wrong bytesPerChecksum while writing", true);
    byteBuf.putInt(checksumPos+1, 512);
    
    byteBuf.putInt(targetPos, -1-random.nextInt(oneMil));
    sendRecvData("bad targets len while writing", true);
    byteBuf.putInt(targetPos, 0);
    
    byteBuf.putLong(blockPos, ++newBlockId);
    int dataChunkPos = byteBuf.position();
    byteBuf.putInt(-1-random.nextInt(oneMil));
    recvByteBuf.position(0);
    recvByteBuf.putShort((short)FSConstants.OP_STATUS_ERROR);//err ret expected.
    sendRecvData("negative DATA_CHUNK len while writing", false);
    byteBuf.putInt(dataChunkPos, 0);
    
    byteBuf.putInt(0); // zero checksum
    byteBuf.putLong(blockPos, ++newBlockId);    
    //ok finally write a block with 0 len
    recvByteBuf.putShort(0, (short)FSConstants.OP_STATUS_SUCCESS);
    sendRecvData("Writing a zero len block", false);
    
    
    /* Test OP_READ_BLOCK */
    
    byteBuf.position(opPos);
    byteBuf.put((byte)FSConstants.OP_READ_BLOCK);
    blockPos = byteBuf.position();
    newBlockId = firstBlock.getBlockId()-1;
    byteBuf.putLong(newBlockId);
    int startOffsetPos = byteBuf.position();
    byteBuf.putLong(0L);
    int lenPos = byteBuf.position();
    byteBuf.putLong(fileLen);
    /* We should change DataNode to return ERROR_INVALID instead of closing 
     * the connection.
     */
    sendRecvData("Wrong block ID for read", true); 
    byteBuf.putLong(blockPos, firstBlock.getBlockId());
    
    recvByteBuf.position(0);
    recvByteBuf.putShort((short)FSConstants.OP_STATUS_ERROR_INVALID);
    byteBuf.putLong(startOffsetPos, -1-random.nextInt(oneMil));
    sendRecvData("Negative start-offset for read", false);
    
    byteBuf.putLong(startOffsetPos, fileLen);
    sendRecvData("Wrong start-offset for read", false);
    byteBuf.putLong(startOffsetPos, 0);
    
    // negative length is ok. Datanode assumes we want to read the whole block.
    recvByteBuf.putShort(0, (short)FSConstants.OP_STATUS_SUCCESS);    
    byteBuf.putLong(lenPos, -1-random.nextInt(oneMil));
    sendRecvData("Negative length for read", false);
    
    recvByteBuf.putShort(0, (short)FSConstants.OP_STATUS_ERROR_INVALID);
    byteBuf.putLong(lenPos, fileLen+1);
    sendRecvData("Wrong length for read", false);
    byteBuf.putLong(lenPos, fileLen);
    
    //At the end of all this, read the file to make sure that succeeds finally.
    readFile(fileSys, file, fileLen);
  }
}

