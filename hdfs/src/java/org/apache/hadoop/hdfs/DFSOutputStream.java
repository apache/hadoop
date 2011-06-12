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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.security.InvalidAccessTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/
class DFSOutputStream extends FSOutputSummer implements Syncable {
  /**
   * 
   */
  private final DFSClient dfsClient;
  private Configuration conf;
  private static final int MAX_PACKETS = 80; // each packet 64K, total 5MB
  private Socket s;
  // closed is accessed by different threads under different locks.
  private volatile boolean closed = false;

  private String src;
  private final long blockSize;
  private final DataChecksum checksum;
  // both dataQueue and ackQueue are protected by dataQueue lock
  private final LinkedList<Packet> dataQueue = new LinkedList<Packet>();
  private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
  private Packet currentPacket = null;
  private DataStreamer streamer;
  private long currentSeqno = 0;
  private long bytesCurBlock = 0; // bytes writen in current block
  private int packetSize = 0; // write packet size, including the header.
  private int chunksPerPacket = 0;
  private volatile IOException lastException = null;
  private long artificialSlowdown = 0;
  private long lastFlushOffset = -1; // offset when flush was invoked
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private volatile boolean appendChunk = false;   // appending to existing partial block
  private long initialFileSize = 0; // at time of file open
  private Progressable progress;
  private short blockReplication; // replication factor of file
  
  private class Packet {
    ByteBuffer buffer;           // only one of buf and buffer is non-null
    byte[]  buf;  
    long    seqno;               // sequencenumber of buffer in block
    long    offsetInBlock;       // offset in block
    boolean lastPacketInBlock;   // is this the last packet in block?
    int     numChunks;           // number of chunks currently in packet
    int     maxChunks;           // max chunks in packet
    int     dataStart;
    int     dataPos;
    int     checksumStart;
    int     checksumPos;      
    private static final long HEART_BEAT_SEQNO = -1L;

    /**
     *  create a heartbeat packet
     */
    Packet() {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = 0;
      this.seqno = HEART_BEAT_SEQNO;
      
      buffer = null;
      int packetSize = DataNode.PKT_HEADER_LEN + DFSClient.SIZE_OF_INTEGER;
      buf = new byte[packetSize];
      
      checksumStart = dataStart = packetSize;
      checksumPos = checksumStart;
      dataPos = dataStart;
      maxChunks = 0;
    }
    
    // create a new packet
    Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = currentSeqno;
      currentSeqno++;
      
      buffer = null;
      buf = new byte[pktSize];
      
      checksumStart = DataNode.PKT_HEADER_LEN + DFSClient.SIZE_OF_INTEGER;
      checksumPos = checksumStart;
      dataStart = checksumStart + chunksPerPkt * checksum.getChecksumSize();
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    void writeData(byte[] inarray, int off, int len) {
      if ( dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void  writeChecksum(byte[] inarray, int off, int len) {
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }
    
    /**
     * Returns ByteBuffer that contains one full packet, including header.
     */
    ByteBuffer getBuffer() {
      /* Once this is called, no more data can be added to the packet.
       * setting 'buf' to null ensures that.
       * This is called only when the packet is ready to be sent.
       */
      if (buffer != null) {
        return buffer;
      }
      
      //prepare the header and close any gap between checksum and data.
      
      int dataLen = dataPos - dataStart;
      int checksumLen = checksumPos - checksumStart;
      
      if (checksumPos != dataStart) {
        /* move the checksum to cover the gap.
         * This can happen for the last packet.
         */
        System.arraycopy(buf, checksumStart, buf, 
                         dataStart - checksumLen , checksumLen); 
      }
      
      int pktLen = DFSClient.SIZE_OF_INTEGER + dataLen + checksumLen;
      
      //normally dataStart == checksumPos, i.e., offset is zero.
      buffer = ByteBuffer.wrap(buf, dataStart - checksumPos,
                               DataNode.PKT_HEADER_LEN + pktLen);
      buf = null;
      buffer.mark();
      
      /* write the header and data length.
       * The format is described in comment before DataNode.BlockSender
       */
      buffer.putInt(pktLen);  // pktSize
      buffer.putLong(offsetInBlock); 
      buffer.putLong(seqno);
      buffer.put((byte) ((lastPacketInBlock) ? 1 : 0));
      //end of pkt header
      buffer.putInt(dataLen); // actual data length, excluding checksum.
      
      buffer.reset();
      return buffer;
    }
    
    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }
    
    /**
     * Check if this packet is a heart beat packet
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }
    
    public String toString() {
      return "packet seqno:" + this.seqno +
      " offsetInBlock:" + this.offsetInBlock + 
      " lastPacketInBlock:" + this.lastPacketInBlock +
      " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
    }
  }

  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  //
  class DataStreamer extends Daemon {
    private volatile boolean streamerClosed = false;
    private Block block; // its length is number of bytes acked
    private BlockAccessToken accessToken;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private ResponseProcessor response = null;
    private volatile DatanodeInfo[] nodes = null; // list of targets for current block
    private ArrayList<DatanodeInfo> excludedNodes = new ArrayList<DatanodeInfo>();
    volatile boolean hasError = false;
    volatile int errorIndex = -1;
    private BlockConstructionStage stage;  // block construction stage
    private long bytesSent = 0; // number of bytes that've been sent

    /**
     * Default construction for file create
     */
    private DataStreamer() {
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }
    
    /**
     * Construct a data streamer for append
     * @param lastBlock last block of the file to be appended
     * @param stat status of the file to be appended
     * @param bytesPerChecksum number of bytes per checksum
     * @throws IOException if error occurs
     */
    private DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
        int bytesPerChecksum) throws IOException {
      stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
      block = lastBlock.getBlock();
      bytesSent = block.getNumBytes();
      accessToken = lastBlock.getAccessToken();
      long usedInLastBlock = stat.getLen() % blockSize;
      int freeInLastBlock = (int)(blockSize - usedInLastBlock);

      // calculate the amount of free space in the pre-existing 
      // last crc chunk
      int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
      int freeInCksum = bytesPerChecksum - usedInCksum;

      // if there is space in the last block, then we have to 
      // append to that block
      if (freeInLastBlock == blockSize) {
        throw new IOException("The last block for file " + 
            src + " is full.");
      }

      if (usedInCksum > 0 && freeInCksum > 0) {
        // if there is space in the last partial chunk, then 
        // setup in such a way that the next packet will have only 
        // one chunk that fills up the partial chunk.
        //
        computePacketChunkSize(0, freeInCksum);
        resetChecksumChunk(freeInCksum);
        appendChunk = true;
      } else {
        // if the remaining space in the block is smaller than 
        // that expected size of of a packet, then create 
        // smaller size packet.
        //
        computePacketChunkSize(Math.min(dfsClient.writePacketSize, freeInLastBlock), 
            bytesPerChecksum);
      }

      // setup pipeline to append to the last block XXX retries??
      nodes = lastBlock.getLocations();
      errorIndex = -1;   // no errors yet.
      if (nodes.length < 1) {
        throw new IOException("Unable to retrieve blocks locations " +
            " for last block " + block +
            "of file " + src);

      }
    }
    
    /**
     * Initialize for data streaming
     */
    private void initDataStreaming() {
      this.setName("DataStreamer for file " + src +
          " block " + block);
      response = new ResponseProcessor(nodes);
      response.start();
      stage = BlockConstructionStage.DATA_STREAMING;
    }
    
    private void endBlock() {
      DFSClient.LOG.debug("Closing old block " + block);
      this.setName("DataStreamer for file " + src);
      closeResponder();
      closeStream();
      nodes = null;
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }
    
    /*
     * streamer thread is the only thread that opens streams to datanode, 
     * and closes them. Any error recovery is also done by this thread.
     */
    public void run() {
      long lastPacket = System.currentTimeMillis();
      while (!streamerClosed && dfsClient.clientRunning) {

        // if the Responder encountered an error, shutdown Responder
        if (hasError && response != null) {
          try {
            response.close();
            response.join();
            response = null;
          } catch (InterruptedException  e) {
          }
        }

        Packet one = null;

        try {
          // process datanode IO errors if any
          boolean doSleep = false;
          if (hasError && errorIndex>=0) {
            doSleep = processDatanodeError();
          }

          synchronized (dataQueue) {
            // wait for a packet to be sent.
            long now = System.currentTimeMillis();
            while ((!streamerClosed && !hasError && dfsClient.clientRunning 
                && dataQueue.size() == 0 && 
                (stage != BlockConstructionStage.DATA_STREAMING || 
                 stage == BlockConstructionStage.DATA_STREAMING && 
                 now - lastPacket < dfsClient.socketTimeout/2)) || doSleep ) {
              long timeout = dfsClient.socketTimeout/2 - (now-lastPacket);
              timeout = timeout <= 0 ? 1000 : timeout;
              timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                 timeout : 1000;
              try {
                dataQueue.wait(timeout);
              } catch (InterruptedException  e) {
              }
              doSleep = false;
              now = System.currentTimeMillis();
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            // get packet to be sent.
            if (dataQueue.isEmpty()) {
              one = new Packet();  // heartbeat packet
            } else {
              one = dataQueue.getFirst(); // regular data packet
            }
          }

          // get new block from namenode.
          if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
            DFSClient.LOG.debug("Allocating new block");
            nodes = nextBlockOutputStream(src);
            initDataStreaming();
          } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
            DFSClient.LOG.debug("Append to block " + block);
            setupPipelineForAppendOrRecovery();
            initDataStreaming();
          }

          long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
          if (lastByteOffsetInBlock > blockSize) {
            throw new IOException("BlockSize " + blockSize +
                " is smaller than data size. " +
                " Offset of packet in block " + 
                lastByteOffsetInBlock +
                " Aborting file " + src);
          }

          if (one.lastPacketInBlock) {
            // wait for all data packets have been successfully acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                try {
                  // wait for acks to arrive from datanodes
                  dataQueue.wait(1000);
                } catch (InterruptedException  e) {
                }
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            stage = BlockConstructionStage.PIPELINE_CLOSE;
          }
          
          // send the packet
          ByteBuffer buf = one.getBuffer();

          synchronized (dataQueue) {
            // move packet from dataQueue to ackQueue
            if (!one.isHeartbeatPacket()) {
              dataQueue.removeFirst();
              ackQueue.addLast(one);
              dataQueue.notifyAll();
            }
          }

          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("DataStreamer block " + block +
                " sending packet " + one);
          }

          // write out data to remote datanode
          blockStream.write(buf.array(), buf.position(), buf.remaining());
          blockStream.flush();
          lastPacket = System.currentTimeMillis();
          
          if (one.isHeartbeatPacket()) {  //heartbeat packet
          }
          
          // update bytesSent
          long tmpBytesSent = one.getLastByteOffsetBlock();
          if (bytesSent < tmpBytesSent) {
            bytesSent = tmpBytesSent;
          }

          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            // wait for the close packet has been acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                dataQueue.wait(1000);// wait for acks to arrive from datanodes
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }

            endBlock();
          }
          if (progress != null) { progress.progress(); }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && dfsClient.clientRunning) {
            Thread.sleep(artificialSlowdown); 
          }
        } catch (Throwable e) {
          DFSClient.LOG.warn("DataStreamer Exception: " + 
              StringUtils.stringifyException(e));
          if (e instanceof IOException) {
            setLastException((IOException)e);
          }
          hasError = true;
          if (errorIndex == -1) { // not a datanode error
            streamerClosed = true;
          }
        }
      }
      closeInternal();
    }

    private void closeInternal() {
      closeResponder();       // close and join
      closeStream();
      streamerClosed = true;
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
    }

    /*
     * close both streamer and DFSOutputStream, should be called only 
     * by an external thread and only after all data to be sent has 
     * been flushed to datanode.
     * 
     * Interrupt this data streamer if force is true
     * 
     * @param force if this data stream is forced to be closed 
     */
    void close(boolean force) {
      streamerClosed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
      if (force) {
        this.interrupt();
      }
    }

    private void closeResponder() {
      if (response != null) {
        try {
          response.close();
          response.join();
        } catch (InterruptedException  e) {
        } finally {
          response = null;
        }
      }
    }

    private void closeStream() {
      if (blockStream != null) {
        try {
          blockStream.close();
        } catch (IOException e) {
        } finally {
          blockStream = null;
        }
      }
      if (blockReplyStream != null) {
        try {
          blockReplyStream.close();
        } catch (IOException e) {
        } finally {
          blockReplyStream = null;
        }
      }
    }

    //
    // Processes reponses from the datanodes.  A packet is removed 
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Daemon {

      private volatile boolean responderClosed = false;
      private DatanodeInfo[] targets = null;
      private boolean isLastPacketInBlock = false;

      ResponseProcessor (DatanodeInfo[] targets) {
        this.targets = targets;
      }

      public void run() {

        setName("ResponseProcessor for block " + block);
        PipelineAck ack = new PipelineAck();

        while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
          // process responses from datanodes.
          try {
            // read an ack from the pipeline
            ack.readFields(blockReplyStream);
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("DFSClient " + ack);
            }
            
            long seqno = ack.getSeqno();
            // processes response status from datanodes.
            for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
              final DataTransferProtocol.Status reply = ack.getReply(i);
              if (reply != SUCCESS) {
                errorIndex = i; // first bad datanode
                throw new IOException("Bad response " + reply +
                    " for block " + block +
                    " from datanode " + 
                    targets[i].getName());
              }
            }
            
            assert seqno != PipelineAck.UNKOWN_SEQNO : 
              "Ack for unkown seqno should be a failed ack: " + ack;
            if (seqno == Packet.HEART_BEAT_SEQNO) {  // a heartbeat ack
              continue;
            }

            // a success ack for a data packet
            Packet one = null;
            synchronized (dataQueue) {
              one = ackQueue.getFirst();
            }
            if (one.seqno != seqno) {
              throw new IOException("Responseprocessor: Expecting seqno " +
                                    " for block " + block +
                                    one.seqno + " but received " + seqno);
            }
            isLastPacketInBlock = one.lastPacketInBlock;
            // update bytesAcked
            block.setNumBytes(one.getLastByteOffsetBlock());

            synchronized (dataQueue) {
              ackQueue.removeFirst();
              dataQueue.notifyAll();
            }
          } catch (Exception e) {
            if (!responderClosed) {
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              hasError = true;
              errorIndex = errorIndex==-1 ? 0 : errorIndex;
              synchronized (dataQueue) {
                dataQueue.notifyAll();
              }
              DFSClient.LOG.warn("DFSOutputStream ResponseProcessor exception " + 
                  " for block " + block +
                  StringUtils.stringifyException(e));
              responderClosed = true;
            }
          }
        }
      }

      void close() {
        responderClosed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError() throws IOException {
      if (response != null) {
        DFSClient.LOG.info("Error Recovery for block " + block +
        " waiting for responder to exit. ");
        return true;
      }
      closeStream();

      // move packets from ack queue to front of the data queue
      synchronized (dataQueue) {
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      boolean doSleep = setupPipelineForAppendOrRecovery();
      
      if (!streamerClosed && dfsClient.clientRunning) {
        if (stage == BlockConstructionStage.PIPELINE_CLOSE) {
          synchronized (dataQueue) {
            dataQueue.remove();  // remove the end of block packet
            dataQueue.notifyAll();
          }
          endBlock();
        } else {
          initDataStreaming();
        }
      }
      
      return doSleep;
    }


    /**
     * Open a DataOutputStream to a DataNode pipeline so that 
     * it can be written to.
     * This happens when a file is appended or data streaming fails
     * It keeps on trying until a pipeline is setup
     */
    private boolean setupPipelineForAppendOrRecovery() throws IOException {
      // check number of datanodes
      if (nodes == null || nodes.length == 0) {
        String msg = "Could not get block locations. " + "Source file \""
            + src + "\" - Aborting...";
        DFSClient.LOG.warn(msg);
        setLastException(new IOException(msg));
        streamerClosed = true;
        return false;
      }
      
      boolean success = false;
      long newGS = 0L;
      while (!success && !streamerClosed && dfsClient.clientRunning) {
        boolean isRecovery = hasError;
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex >= 0) {
          StringBuilder pipelineMsg = new StringBuilder();
          for (int j = 0; j < nodes.length; j++) {
            pipelineMsg.append(nodes[j].getName());
            if (j < nodes.length - 1) {
              pipelineMsg.append(", ");
            }
          }
          if (nodes.length <= 1) {
            lastException = new IOException("All datanodes " + pipelineMsg
                + " are bad. Aborting...");
            streamerClosed = true;
            return false;
          }
          DFSClient.LOG.warn("Error Recovery for block " + block +
              " in pipeline " + pipelineMsg + 
              ": bad datanode " + nodes[errorIndex].getName());
          DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
          System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
          System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
              newnodes.length-errorIndex);
          nodes = newnodes;
          hasError = false;
          lastException = null;
          errorIndex = -1;
        }

        // get a new generation stamp and an access token
        LocatedBlock lb = dfsClient.namenode.updateBlockForPipeline(block, dfsClient.clientName);
        newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getAccessToken();
        
        // set up the pipeline again with the remaining nodes
        success = createBlockOutputStream(nodes, newGS, isRecovery);
      }

      if (success) {
        // update pipeline at the namenode
        Block newBlock = new Block(
            block.getBlockId(), block.getNumBytes(), newGS);
        dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock, nodes);
        // update client side generation stamp
        block = newBlock;
      }
      return false; // do not sleep, continue processing
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     */
    private DatanodeInfo[] nextBlockOutputStream(String client) throws IOException {
      LocatedBlock lb = null;
      DatanodeInfo[] nodes = null;
      int count = conf.getInt("dfs.client.block.write.retries", 3);
      boolean success = false;
      do {
        hasError = false;
        lastException = null;
        errorIndex = -1;
        success = false;

        long startTime = System.currentTimeMillis();
        DatanodeInfo[] w = excludedNodes.toArray(
            new DatanodeInfo[excludedNodes.size()]);
        lb = locateFollowingBlock(startTime, w.length > 0 ? w : null);
        block = lb.getBlock();
        block.setNumBytes(0);
        accessToken = lb.getAccessToken();
        nodes = lb.getLocations();

        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, 0L, false);

        if (!success) {
          DFSClient.LOG.info("Abandoning block " + block);
          dfsClient.namenode.abandonBlock(block, src, dfsClient.clientName);
          block = null;
          DFSClient.LOG.info("Excluding datanode " + nodes[errorIndex]);
          excludedNodes.add(nodes[errorIndex]);
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return nodes;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    private boolean createBlockOutputStream(DatanodeInfo[] nodes, long newGS,
        boolean recoveryFlag) {
      DataTransferProtocol.Status pipelineStatus = SUCCESS;
      String firstBadLink = "";
      if (DFSClient.LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          DFSClient.LOG.debug("pipeline = " + nodes[i].getName());
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks.set(true);

      try {
        DFSClient.LOG.debug("Connecting to " + nodes[0].getName());
        InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getName());
        s = dfsClient.socketFactory.createSocket();
        int timeoutValue = dfsClient.getDatanodeReadTimeout(nodes.length);
        NetUtils.connect(s, target, timeoutValue);
        s.setSoTimeout(timeoutValue);
        s.setSendBufferSize(DFSClient.DEFAULT_DATA_SOCKET_SIZE);
        DFSClient.LOG.debug("Send buf size " + s.getSendBufferSize());
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);

        //
        // Xmit header info to datanode
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
            NetUtils.getOutputStream(s, writeTimeout),
            DataNode.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(NetUtils.getInputStream(s));

        // send the request
        DataTransferProtocol.Sender.opWriteBlock(out,
            block.getBlockId(), block.getGenerationStamp(),
            nodes.length, recoveryFlag?stage.getRecoveryStage():stage, newGS,
            block.getNumBytes(), bytesSent, dfsClient.clientName, null, nodes, accessToken);
        checksum.writeHeader(out);
        out.flush();

        // receive ack for connect
        pipelineStatus = DataTransferProtocol.Status.read(blockReplyStream);
        firstBadLink = Text.readString(blockReplyStream);
        if (pipelineStatus != SUCCESS) {
          if (pipelineStatus == ERROR_ACCESS_TOKEN) {
            throw new InvalidAccessTokenException(
                "Got access token error for connect ack with firstBadLink as "
                    + firstBadLink);
          } else {
            throw new IOException("Bad connect ack with firstBadLink as "
                + firstBadLink);
          }
        }

        blockStream = out;
        return true; // success

      } catch (IOException ie) {

        DFSClient.LOG.info("Exception in createBlockOutputStream " + ie);

        // find the datanode that matches
        if (firstBadLink.length() != 0) {
          for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getName().equals(firstBadLink)) {
              errorIndex = i;
              break;
            }
          }
        } else {
          errorIndex = 0;
        }
        hasError = true;
        setLastException(ie);
        blockReplyStream = null;
        return false;  // error
      }
    }

    private LocatedBlock locateFollowingBlock(long start,
        DatanodeInfo[] excludedNodes) 
        throws IOException, UnresolvedLinkException {
      int retries = conf.getInt("dfs.client.block.write.locateFollowingBlock.retries", 5);
      long sleeptime = 400;
      while (true) {
        long localstart = System.currentTimeMillis();
        while (true) {
          try {
            return dfsClient.namenode.addBlock(src, dfsClient.clientName, block, excludedNodes);
          } catch (RemoteException e) {
            IOException ue = 
              e.unwrapRemoteException(FileNotFoundException.class,
                                      AccessControlException.class,
                                      NSQuotaExceededException.class,
                                      DSQuotaExceededException.class,
                                      UnresolvedPathException.class);
            if (ue != e) { 
              throw ue; // no need to retry these exceptions
            }
            
            
            if (NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {
              if (retries == 0) { 
                throw e;
              } else {
                --retries;
                DFSClient.LOG.info(StringUtils.stringifyException(e));
                if (System.currentTimeMillis() - localstart > 5000) {
                  DFSClient.LOG.info("Waiting for replication for "
                      + (System.currentTimeMillis() - localstart) / 1000
                      + " seconds");
                }
                try {
                  DFSClient.LOG.warn("NotReplicatedYetException sleeping " + src
                      + " retries left " + retries);
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                }
              }
            } else {
              throw e;
            }

          }
        }
      } 
    }

    Block getBlock() {
      return block;
    }

    DatanodeInfo[] getNodes() {
      return nodes;
    }

    BlockAccessToken getAccessToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      if (lastException == null) {
        lastException = e;
      }
    }
  }

  private void isClosed() throws IOException {
    if (closed) {
      IOException e = lastException;
      throw e != null ? e : new IOException("DFSOutputStream is closed");
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  synchronized DatanodeInfo[] getPipeline() {
    if (streamer == null) {
      return null;
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return null;
    }
    DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
    for (int i = 0; i < currentNodes.length; i++) {
      value[i] = currentNodes[i];
    }
    return value;
  }

  private DFSOutputStream(DFSClient dfsClient, String src, long blockSize, Progressable progress,
      int bytesPerChecksum, short replication) throws IOException {
    super(new PureJavaCrc32(), bytesPerChecksum, 4);
    this.dfsClient = dfsClient;
    this.conf = dfsClient.conf;
    this.src = src;
    this.blockSize = blockSize;
    this.blockReplication = replication;
    this.progress = progress;
    if (progress != null) {
      DFSClient.LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
    }
    
    if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                            ") and blockSize(" + blockSize + 
                            ") do not match. " + "blockSize should be a " +
                            "multiple of io.bytes.per.checksum");
                            
    }
    checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 
                                            bytesPerChecksum);
  }

  /**
   * Create a new output stream to the given DataNode.
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable, boolean, short, long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, FsPermission masked, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize, Progressable progress,
      int buffersize, int bytesPerChecksum) 
      throws IOException, UnresolvedLinkException {
    this(dfsClient, src, blockSize, progress, bytesPerChecksum, replication);

    computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);

    try {
      dfsClient.namenode.create(
          src, masked, dfsClient.clientName, new EnumSetWritable<CreateFlag>(flag), createParent, replication, blockSize);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
    streamer = new DataStreamer();
    streamer.start();
  }

  /**
   * Create a new output stream to the given DataNode.
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, int buffersize, Progressable progress,
      LocatedBlock lastBlock, HdfsFileStatus stat,
      int bytesPerChecksum) throws IOException {
    this(dfsClient, src, stat.getBlockSize(), progress, bytesPerChecksum, stat.getReplication());
    initialFileSize = stat.getLen(); // length of file when opened

    //
    // The last partial block of the file has to be filled.
    //
    if (lastBlock != null) {
      // indicate that we are appending to an existing block
      bytesCurBlock = lastBlock.getBlockSize();
      streamer = new DataStreamer(lastBlock, stat, bytesPerChecksum);
    } else {
      computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);
      streamer = new DataStreamer();
    }
    streamer.start();
  }

  private void computePacketChunkSize(int psize, int csize) {
    int chunkSize = csize + checksum.getChecksumSize();
    int n = DataNode.PKT_HEADER_LEN + DFSClient.SIZE_OF_INTEGER;
    chunksPerPacket = Math.max((psize - n + chunkSize-1)/chunkSize, 1);
    packetSize = n + chunkSize*chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
                ", chunkSize=" + chunkSize +
                ", chunksPerPacket=" + chunksPerPacket +
                ", packetSize=" + packetSize);
    }
  }

  private void queuePacket(Packet packet) {
    synchronized (dataQueue) {
      dataQueue.addLast(packet);
      dataQueue.notifyAll();
    }
  }

  private void waitAndQueuePacket(Packet packet) throws IOException {
    synchronized (dataQueue) {
      // If queue is full, then wait till we have enough space
      while (!closed && dataQueue.size() + ackQueue.size()  > MAX_PACKETS) {
        try {
          dataQueue.wait();
        } catch (InterruptedException  e) {
        }
      }
      isClosed();
      queuePacket(packet);
    }
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum) 
                                                        throws IOException {
    dfsClient.checkOpen();
    isClosed();

    int cklen = checksum.length;
    int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
                            " is larger than supported  bytesPerChecksum " +
                            bytesPerChecksum);
    }
    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
                            this.checksum.getChecksumSize() + 
                            " but found to be " + checksum.length);
    }

    if (currentPacket == null) {
      currentPacket = new Packet(packetSize, chunksPerPacket, 
          bytesCurBlock);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
            currentPacket.seqno +
            ", src=" + src +
            ", packetSize=" + packetSize +
            ", chunksPerPacket=" + chunksPerPacket +
            ", bytesCurBlock=" + bytesCurBlock);
      }
    }

    currentPacket.writeChecksum(checksum, 0, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.numChunks++;
    bytesCurBlock += len;

    // If packet is full, enqueue it for transmission
    //
    if (currentPacket.numChunks == currentPacket.maxChunks ||
        bytesCurBlock == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
            currentPacket.seqno +
            ", src=" + src +
            ", bytesCurBlock=" + bytesCurBlock +
            ", blockSize=" + blockSize +
            ", appendChunk=" + appendChunk);
      }
      waitAndQueuePacket(currentPacket);
      currentPacket = null;

      // If the reopened file did not end at chunk boundary and the above
      // write filled up its partial chunk. Tell the summer to generate full 
      // crc chunks from now on.
      if (appendChunk && bytesCurBlock%bytesPerChecksum == 0) {
        appendChunk = false;
        resetChecksumChunk(bytesPerChecksum);
      }

      if (!appendChunk) {
        int psize = Math.min((int)(blockSize-bytesCurBlock), dfsClient.writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
      }
      //
      // if encountering a block boundary, send an empty packet to 
      // indicate the end of block and reset bytesCurBlock.
      //
      if (bytesCurBlock == blockSize) {
        currentPacket = new Packet(DataNode.PKT_HEADER_LEN+4, 0, 
            bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
        waitAndQueuePacket(currentPacket);
        currentPacket = null;
        bytesCurBlock = 0;
        lastFlushOffset = -1;
      }
    }
  }

  @Override
  @Deprecated
  public synchronized void sync() throws IOException {
    hflush();
  }
  
  /**
   * flushes out to all replicas of the block. 
   * The data is in the buffers of the DNs 
   * but not neccessary on the DN's OS buffers. 
   *
   * It is a synchronous operation. When it returns,
   * it gurantees that flushed data become visible to new readers. 
   * It is not guaranteed that data has been flushed to 
   * persistent store on the datanode. 
   * Block allocations are persisted on namenode.
   */
  @Override
  public synchronized void hflush() throws IOException {
    dfsClient.checkOpen();
    isClosed();
    try {
      /* Record current blockOffset. This might be changed inside
       * flushBuffer() where a partial checksum chunk might be flushed.
       * After the flush, reset the bytesCurBlock back to its previous value,
       * any partial checksum chunk will be sent now and in next packet.
       */
      long saveOffset = bytesCurBlock;

      // flush checksum buffer, but keep checksum buffer intact
      flushBuffer(true);

      DFSClient.LOG.debug("DFSClient flush() : saveOffset " + saveOffset +  
                " bytesCurBlock " + bytesCurBlock +
                " lastFlushOffset " + lastFlushOffset);
      
      // Flush only if we haven't already flushed till this offset.
      if (lastFlushOffset != bytesCurBlock) {

        // record the valid offset of this flush
        lastFlushOffset = bytesCurBlock;

        // wait for all packets to be sent and acknowledged
        flushInternal();
      } else {
        // just discard the current packet since it is already been sent.
        currentPacket = null;
      }
      
      // Restore state of stream. Record the last flush offset 
      // of the last full chunk that was flushed.
      //
      bytesCurBlock = saveOffset;

      // If any new blocks were allocated since the last flush, 
      // then persist block locations on namenode. 
      //
      if (persistBlocks.getAndSet(false)) {
        dfsClient.namenode.fsync(src, dfsClient.clientName);
      }
    } catch (IOException e) {
        lastException = new IOException("IOException flush:" + e);
        closeThreads(true);
        throw e;
    }
  }

  /**
   * The expected semantics is all data have flushed out to all replicas 
   * and all replicas have done posix fsync equivalent - ie the OS has 
   * flushed it to the disk device (but the disk may have it in its cache).
   * 
   * Right now by default it is implemented as hflush
   */
  @Override
  public synchronized void hsync() throws IOException {
    hflush();
  }

  /**
   * Returns the number of replicas of current block. This can be different
   * from the designated replication factor of the file because the NameNode
   * does not replicate the block to which a client is currently writing to.
   * The client continues to write to a block even if a few datanodes in the
   * write pipeline have failed. 
   * @return the number of valid replicas of the current block
   */
  public synchronized int getNumCurrentReplicas() throws IOException {
    dfsClient.checkOpen();
    isClosed();
    if (streamer == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    return currentNodes.length;
  }
  
  /**
   * Waits till all existing data is flushed and confirmations 
   * received from datanodes. 
   */
  private synchronized void flushInternal() throws IOException {
    dfsClient.checkOpen();
    isClosed();
    //
    // If there is data in the current buffer, send it across
    //
    if (currentPacket != null) {
      queuePacket(currentPacket);
      currentPacket = null;
    }

    synchronized (dataQueue) {
      while (!closed && dataQueue.size() + ackQueue.size() > 0) {
        try {
          dataQueue.wait();
        } catch (InterruptedException  e) {
        }
      }
      isClosed();
    }
  }

  /**
   * Aborts this output stream and releases any system 
   * resources associated with this stream.
   */
  synchronized void abort() throws IOException {
    if (closed) {
      return;
    }
    streamer.setLastException(new IOException("Lease timeout of " +
                             (dfsClient.hdfsTimeout/1000) + " seconds expired."));
    closeThreads(true);
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  private void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      if (s != null) {
        s.close();
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer = null;
      s = null;
      closed = true;
    }
  }
  
  /**
   * Closes this output stream and releases any system 
   * resources associated with this stream.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      IOException e = lastException;
      if (e == null)
        return;
      else
        throw e;
    }

    try {
      flushBuffer();       // flush from all upper layers

      if (currentPacket != null) { 
        waitAndQueuePacket(currentPacket);
      }

      if (bytesCurBlock != 0) {
        // send an empty packet to mark the end of the block
        currentPacket = new Packet(DataNode.PKT_HEADER_LEN+4, 0, 
            bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
      }

      flushInternal();             // flush all data to Datanodes
      // get last block before destroying the streamer
      Block lastBlock = streamer.getBlock();
      closeThreads(false);
      completeFile(lastBlock);
      dfsClient.leasechecker.remove(src);
    } finally {
      closed = true;
    }
  }

  // should be called holding (this) lock since setTestFilename() may 
  // be called during unit tests
  private void completeFile(Block last) throws IOException {
    long localstart = System.currentTimeMillis();
    boolean fileComplete = false;
    while (!fileComplete) {
      fileComplete = dfsClient.namenode.complete(src, dfsClient.clientName, last);
      if (!fileComplete) {
        if (!dfsClient.clientRunning ||
              (dfsClient.hdfsTimeout > 0 &&
               localstart + dfsClient.hdfsTimeout < System.currentTimeMillis())) {
            String msg = "Unable to close file because dfsclient " +
                          " was unable to contact the HDFS servers." +
                          " clientRunning " + dfsClient.clientRunning +
                          " hdfsTimeout " + dfsClient.hdfsTimeout;
            DFSClient.LOG.info(msg);
            throw new IOException(msg);
        }
        try {
          Thread.sleep(400);
          if (System.currentTimeMillis() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete file " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = DataNode.PKT_HEADER_LEN + DFSClient.SIZE_OF_INTEGER +
                 (checksum.getBytesPerChecksum() + 
                  checksum.getChecksumSize()) * chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  long getInitialLen() {
    return initialFileSize;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  BlockAccessToken getAccessToken() {
    return streamer.getAccessToken();
  }

}
