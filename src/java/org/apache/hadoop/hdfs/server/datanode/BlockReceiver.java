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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements java.io.Closeable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private Block block; // the block to receive
  private DataInputStream in = null; // from where data are read
  private DataChecksum checksum; // from where chunks of a block can be read
  private OutputStream out = null; // to block file at local disk
  private DataOutputStream checksumOut = null; // to crc file at local disk
  private int bytesPerChecksum;
  private int checksumSize;
  private ByteBuffer buf; // contains one full packet.
  private int bufRead; //amount of valid data in the buf
  private int maxPacketReadLen;
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private BlockTransferThrottler throttler;
  private FSDataset.BlockWriteStreams streams;
  private String clientName;
  DatanodeInfo srcDataNode = null;
  private Checksum partialCrc = null;
  private final DataNode datanode;
  final private ReplicaInPipelineInterface replicaInfo;
  volatile private boolean mirrorError;

  BlockReceiver(Block block, DataInputStream in, String inAddr,
                String myAddr, BlockConstructionStage stage, 
                long newGs, long minBytesRcvd, long maxBytesRcvd, 
                String clientName, DatanodeInfo srcDataNode, DataNode datanode)
                throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.clientName = clientName;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;
      //
      // Open local disk out
      //
      if (clientName.length() == 0) { //replication or move
        replicaInfo = datanode.data.createTemporary(block);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaInfo = datanode.data.createRbw(block);
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaInfo = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaInfo = datanode.data.append(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block);
          }
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaInfo = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block);
          }
          block.setGenerationStamp(newGs);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      // read checksum meta information
      this.checksum = DataChecksum.newDataChecksum(in);
      this.bytesPerChecksum = checksum.getBytesPerChecksum();
      this.checksumSize = checksum.getChecksumSize();
      
      boolean isCreate = stage == BlockConstructionStage.PIPELINE_SETUP_CREATE 
      || clientName.length() == 0;
      streams = replicaInfo.createStreams(isCreate,
          this.bytesPerChecksum, this.checksumSize);
      if (streams != null) {
        this.out = streams.dataOut;
        this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                                                  streams.checksumOut, 
                                                  SMALL_BUFFER_SIZE));
        
        // write data chunk header if creating a new replica
        if (isCreate) {
          BlockMetadataHeader.writeHeader(checksumOut, checksum);
        } 
      }
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = FSDataset.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
          cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;
        datanode.checkDiskError(ioe); // may throw an exception here
      }
      
      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  /**
   * close files.
   */
  public void close() throws IOException {

    IOException ioe = null;
    // close checksum file
    try {
      if (checksumOut != null) {
        checksumOut.flush();
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    // close block file
    try {
      if (out != null) {
        out.flush();
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    }
    // disk check
    if(ioe != null) {
      datanode.checkDiskError(ioe);
      throw ioe;
    }
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flush() throws IOException {
    if (checksumOut != null) {
      checksumOut.flush();
    }
    if (out != null) {
      out.flush();
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    LOG.info(datanode.dnRegistration + ":Exception writing block " +
             block + " to mirror " + mirrorAddr + "\n" +
             StringUtils.stringifyException(ioe));
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks( byte[] dataBuf, int dataOff, int len, 
                             byte[] checksumBuf, int checksumOff ) 
                             throws IOException {
    while (len > 0) {
      int chunkLen = Math.min(len, bytesPerChecksum);
      
      checksum.update(dataBuf, dataOff, chunkLen);

      if (!checksum.compare(checksumBuf, checksumOff)) {
        if (srcDataNode != null) {
          try {
            LOG.info("report corrupt block " + block + " from datanode " +
                      srcDataNode + " to namenode");
            LocatedBlock lb = new LocatedBlock(block, 
                                            new DatanodeInfo[] {srcDataNode});
            datanode.namenode.reportBadBlocks(new LocatedBlock[] {lb});
          } catch (IOException e) {
            LOG.warn("Failed to report bad block " + block + 
                      " from datanode " + srcDataNode + " to namenode");
          }
        }
        throw new IOException("Unexpected checksum mismatch " + 
                              "while writing " + block + " from " + inAddr);
      }

      checksum.reset();
      dataOff += chunkLen;
      checksumOff += checksumSize;
      len -= chunkLen;
    }
  }

  /**
   * Makes sure buf.position() is zero without modifying buf.remaining().
   * It moves the data if position needs to be changed.
   */
  private void shiftBufData() {
    if (bufRead != buf.limit()) {
      throw new IllegalStateException("bufRead should be same as " +
                                      "buf.limit()");
    }
    
    //shift the remaining data on buf to the front
    if (buf.position() > 0) {
      int dataLeft = buf.remaining();
      if (dataLeft > 0) {
        byte[] b = buf.array();
        System.arraycopy(b, buf.position(), b, 0, dataLeft);
      }
      buf.position(0);
      bufRead = dataLeft;
      buf.limit(bufRead);
    }
  }
  
  /**
   * reads upto toRead byte to buf at buf.limit() and increments the limit.
   * throws an IOException if read does not succeed.
   */
  private int readToBuf(int toRead) throws IOException {
    if (toRead < 0) {
      toRead = (maxPacketReadLen > 0 ? maxPacketReadLen : buf.capacity())
               - buf.limit();
    }
    
    int nRead = in.read(buf.array(), buf.limit(), toRead);
    
    if (nRead < 0) {
      throw new EOFException("while trying to read " + toRead + " bytes");
    }
    bufRead = buf.limit() + nRead;
    buf.limit(bufRead);
    return nRead;
  }
  
  
  /**
   * Reads (at least) one packet and returns the packet length.
   * buf.position() points to the start of the packet and 
   * buf.limit() point to the end of the packet. There could 
   * be more data from next packet in buf.<br><br>
   * 
   * It tries to read a full packet with single read call.
   * Consecutive packets are usually of the same length.
   */
  private void readNextPacket() throws IOException {
    /* This dances around buf a little bit, mainly to read 
     * full packet with single read and to accept arbitarary size  
     * for next packet at the same time.
     */
    if (buf == null) {
      /* initialize buffer to the best guess size:
       * 'chunksPerPacket' calculation here should match the same 
       * calculation in DFSClient to make the guess accurate.
       */
      int chunkSize = bytesPerChecksum + checksumSize;
      int chunksPerPacket = (datanode.writePacketSize - DataNode.PKT_HEADER_LEN - 
                             SIZE_OF_INTEGER + chunkSize - 1)/chunkSize;
      buf = ByteBuffer.allocate(DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER +
                                Math.max(chunksPerPacket, 1) * chunkSize);
      buf.limit(0);
    }
    
    // See if there is data left in the buffer :
    if (bufRead > buf.limit()) {
      buf.limit(bufRead);
    }
    
    while (buf.remaining() < SIZE_OF_INTEGER) {
      if (buf.position() > 0) {
        shiftBufData();
      }
      readToBuf(-1);
    }
    
    /* We mostly have the full packet or at least enough for an int
     */
    buf.mark();
    int payloadLen = buf.getInt();
    buf.reset();
    
    // check corrupt values for pktLen, 100MB upper limit should be ok?
    if (payloadLen < 0 || payloadLen > (100*1024*1024)) {
      throw new IOException("Incorrect value for packet payload : " +
                            payloadLen);
    }
    
    int pktSize = payloadLen + DataNode.PKT_HEADER_LEN;
    
    if (buf.remaining() < pktSize) {
      //we need to read more data
      int toRead = pktSize - buf.remaining();
      
      // first make sure buf has enough space.        
      int spaceLeft = buf.capacity() - buf.limit();
      if (toRead > spaceLeft && buf.position() > 0) {
        shiftBufData();
        spaceLeft = buf.capacity() - buf.limit();
      }
      if (toRead > spaceLeft) {
        byte oldBuf[] = buf.array();
        int toCopy = buf.limit();
        buf = ByteBuffer.allocate(toCopy + toRead);
        System.arraycopy(oldBuf, 0, buf.array(), 0, toCopy);
        buf.limit(toCopy);
      }
      
      //now read:
      while (toRead > 0) {
        toRead -= readToBuf(toRead);
      }
    }
    
    if (buf.remaining() > pktSize) {
      buf.limit(buf.position() + pktSize);
    }
    
    if (pktSize > maxPacketReadLen) {
      maxPacketReadLen = pktSize;
    }
  }
  
  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  private int receivePacket() throws IOException {
    // read the next packet
    readNextPacket();
    
    buf.mark();
    //read the header
    buf.getInt(); // packet length
    long offsetInBlock = buf.getLong(); // get offset of packet in block
    
    if (offsetInBlock > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + 
          "from " + inAddr + " at offset " + offsetInBlock +
          ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    long seqno = buf.getLong();    // get seqno
    boolean lastPacketInBlock = (buf.get() != 0);
    
    int len = buf.getInt();
    if (len < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            offsetInBlock + ": " + len); 
    } 
    int endOfHeader = buf.position();
    buf.reset();
    
    return receivePacket(offsetInBlock, seqno, lastPacketInBlock, len, endOfHeader);
  }

  /**
   * Write the received packet to disk (data only)
   */
  private void writePacketToDisk(byte[] pktBuf, int startByteToDisk, 
      int numBytesToDisk) throws IOException {
    out.write(pktBuf, startByteToDisk, numBytesToDisk);
  }
  
  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  private int receivePacket(long offsetInBlock, long seqno,
      boolean lastPacketInBlock, int len, int endOfHeader) throws IOException {
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                " of length " + len +
                " seqno " + seqno +
                " offsetInBlock " + offsetInBlock +
                " lastPacketInBlock " + lastPacketInBlock);
    }
    
    // update received bytes
    long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    
    // put in queue for pending acks
    if (responder != null) {
      ((PacketResponder)responder.getRunnable()).enqueue(seqno,
                                      lastPacketInBlock, offsetInBlock); 
    }  

    //First write the packet to the mirror:
    if (mirrorOut != null && !mirrorError) {
      try {
        mirrorOut.write(buf.array(), buf.position(), buf.remaining());
        mirrorOut.flush();
      } catch (IOException e) {
        handleMirrorOutError(e);
      }
    }
    
    buf.position(endOfHeader);        
    
    if (lastPacketInBlock || len == 0) {
      LOG.debug("Receiving an empty packet or the end of the block " + block);
    } else {
      int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
                                                            checksumSize;

      if ( buf.remaining() != (checksumLen + len)) {
        throw new IOException("Data remaining in packet does not match" +
                              "sum of checksumLen and dataLen " +
                              " size remaining: " + buf.remaining() +
                              " data len: " + len +
                              " checksum Len: " + checksumLen);
      }
      int checksumOff = buf.position();
      int dataOff = checksumOff + checksumLen;
      byte pktBuf[] = buf.array();

      buf.position(buf.limit()); // move to the end of the data.

      /* skip verifying checksum iff this is not the last one in the 
       * pipeline and clientName is non-null. i.e. Checksum is verified
       * on all the datanodes when the data is being written by a 
       * datanode rather than a client. Whe client is writing the data, 
       * protocol includes acks and only the last datanode needs to verify 
       * checksum.
       */
      if (mirrorOut == null || clientName.length() == 0) {
        verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff);
      }

      byte[] lastChunkChecksum;
      
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          //finally write to the disk :
          
          if (onDiskLen % bytesPerChecksum != 0) { 
            // prepare to overwrite last checksum
            adjustCrcFilePosition();
          }
          
          // If this is a partial chunk, then read in pre-existing checksum
          if (firstByteInBlock % bytesPerChecksum != 0) {
            LOG.info("Packet starts at " + firstByteInBlock +
                     " for block " + block +
                     " which is not a multiple of bytesPerChecksum " +
                     bytesPerChecksum);
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            computePartialChunkCrc(onDiskLen, offsetInChecksum, bytesPerChecksum);
          }

          int startByteToDisk = dataOff+(int)(onDiskLen-firstByteInBlock);
          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          writePacketToDisk(pktBuf, startByteToDisk, numBytesToDisk);

          // If this is a partial chunk, then verify that this is the only
          // chunk in the packet. Calculate new crc for this chunk.
          if (partialCrc != null) {
            if (len > bytesPerChecksum) {
              throw new IOException("Got wrong length during writeBlock(" + 
                                    block + ") from " + inAddr + " " +
                                    "A packet can have only one partial chunk."+
                                    " len = " + len + 
                                    " bytesPerChecksum " + bytesPerChecksum);
            }
            partialCrc.update(pktBuf, startByteToDisk, numBytesToDisk);
            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
            lastChunkChecksum = Arrays.copyOfRange(
              buf, buf.length - checksumSize, buf.length
            );
            checksumOut.write(buf);
            LOG.debug("Writing out partial crc for data len " + len);
            partialCrc = null;
          } else {
            lastChunkChecksum = Arrays.copyOfRange(
              pktBuf, 
              checksumOff + checksumLen - checksumSize, 
              checksumOff + checksumLen
            );
            checksumOut.write(pktBuf, checksumOff, checksumLen);
          }
          /// flush entire packet
          flush();
          
          replicaInfo.setLastChecksumAndDataLen(
            offsetInBlock, lastChunkChecksum
          );
          
          datanode.myMetrics.bytesWritten.inc(len);
        }
      } catch (IOException iex) {
        datanode.checkDiskError(iex);
        throw iex;
      }
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }
    
    return lastPacketInBlock?-1:len;
  }

  void writeChecksumHeader(DataOutputStream mirrorOut) throws IOException {
    checksum.writeHeader(mirrorOut);
  }
 

  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, BlockTransferThrottler throttlerArg,
      int numTargets) throws IOException {

      boolean responderClosed = false;
      mirrorOut = mirrOut;
      mirrorAddr = mirrAddr;
      throttler = throttlerArg;

    try {
      if (clientName.length() > 0) {
        responder = new Daemon(datanode.threadGroup, 
                               new PacketResponder(this, block, mirrIn, 
                                                   replyOut, numTargets,
                                                   Thread.currentThread()));
        responder.start(); // start thread to processes reponses
      }

      /* 
       * Receive until the last packet.
       */
      while (receivePacket() >= 0) {}

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // if this write is for a replication request (and not
      // from a client), then finalize block. For client-writes, 
      // the block is finalized in the PacketResponder.
      if (clientName.length() == 0) {
        // close the block/crc files
        close();

        // Finalize the block. Does this fsync()?
        block.setNumBytes(replicaInfo.getNumBytes());
        datanode.data.finalizeBlock(block);
        datanode.myMetrics.blocksWritten.inc();
      }

    } catch (IOException ioe) {
      LOG.info("Exception in receiveBlock for block " + block + 
               " " + ioe);
      throw ioe;
    } finally {
      if (!responderClosed) { // Abnormal termination of the flow above
        IOUtils.closeStream(this);
        if (responder != null) {
          responder.interrupt();
        }
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.join();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted receiveBlock");
        }
        responder = null;
      }
    }
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (clientName.length() == 0) { // not client write
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  private void adjustCrcFilePosition() throws IOException {
    if (out != null) {
     out.flush();
    }
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  private void computePartialChunkCrc(long blkoff, long ckoff, 
                                      int bytesPerChecksum) throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    int checksumSize = checksum.getChecksumSize();
    blkoff = blkoff - sizePartialChunk;
    LOG.info("computePartialChunkCrc sizePartialChunk " + 
              sizePartialChunk +
              " block " + block +
              " offset in block " + blkoff +
              " offset in metafile " + ckoff);

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    FSDataset.BlockInputStreams instr = null;
    try { 
      instr = datanode.data.getTmpInputStreams(block, blkoff, ckoff);
      IOUtils.readFully(instr.dataIn, buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(instr.checksumIn, crcbuf, 0, crcbuf.length);
    } finally {
      IOUtils.closeStream(instr);
    }

    // compute crc of partial chunk from data read in the block file.
    partialCrc = new PureJavaCrc32();
    partialCrc.update(buf, 0, sizePartialChunk);
    LOG.info("Read in partial CRC chunk from disk for block " + block);

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != FSInputChecker.checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   FSInputChecker.checksum2long(crcbuf);
      throw new IOException(msg);
    }
    //LOG.debug("Partial CRC matches 0x" + 
    //            Long.toHexString(partialCrc.getValue()));
  }
  
  
  /**
   * Processed responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, FSConstants {   

    //packet waiting for ack
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    private volatile boolean running = true;
    private Block block;
    DataInputStream mirrorIn;   // input from downstream datanode
    DataOutputStream replyOut;  // output to upstream datanode
    private int numTargets;     // number of downstream datanodes including myself
    private BlockReceiver receiver; // The owner of this responder.
    private Thread receiverThread; // the thread that spawns this responder

    public String toString() {
      return "PacketResponder " + numTargets + " for Block " + this.block;
    }

    PacketResponder(BlockReceiver receiver, Block b, DataInputStream in, 
                    DataOutputStream out, int numTargets,
                    Thread receiverThread) {
      this.receiverThread = receiverThread;
      this.receiver = receiver;
      this.block = b;
      mirrorIn = in;
      replyOut = out;
      this.numTargets = numTargets;
    }

    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno
     * @param lastPacketInBlock
     * @param lastByteInPacket
     */
    synchronized void enqueue(long seqno, boolean lastPacketInBlock, long lastByteInPacket) {
      if (running) {
        LOG.debug("PacketResponder " + numTargets + " adding seqno " + seqno +
                  " to ack queue.");
        ackQueue.addLast(new Packet(seqno, lastPacketInBlock, lastByteInPacket));
        notifyAll();
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    synchronized void close() {
      while (running && ackQueue.size() != 0 && datanode.shouldRun) {
        try {
          wait();
        } catch (InterruptedException e) {
          running = false;
        }
      }
      LOG.debug("PacketResponder " + numTargets +
               " for block " + block + " Closing down.");
      running = false;
      notifyAll();
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    public void run() {
      boolean lastPacketInBlock = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (running && datanode.shouldRun && !lastPacketInBlock) {

        boolean isInterrupted = false;
        try {
            Packet pkt = null;
            long expected = -2;
            PipelineAck ack = new PipelineAck();
            long seqno = PipelineAck.UNKOWN_SEQNO;
            try {
              if (numTargets != 0 && !mirrorError) {// not the last DN & no mirror error
                // read an ack from downstream datanode
                ack.readFields(mirrorIn);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("PacketResponder " + numTargets + " got " + ack);
                }
                seqno = ack.getSeqno();
              }
              if (seqno != PipelineAck.UNKOWN_SEQNO || numTargets == 0) {
                synchronized (this) {
                  while (running && datanode.shouldRun && ackQueue.size() == 0) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("PacketResponder " + numTargets + 
                                " seqno = " + seqno +
                                " for block " + block +
                                " waiting for local datanode to finish write.");
                    }
                    wait();
                  }
                  if (!running || !datanode.shouldRun) {
                    break;
                  }
                  pkt = ackQueue.getFirst();
                  expected = pkt.seqno;
                  if (numTargets > 0 && seqno != expected) {
                    throw new IOException("PacketResponder " + numTargets +
                                          " for block " + block +
                                          " expected seqno:" + expected +
                                          " received:" + seqno);
                  }
                  lastPacketInBlock = pkt.lastPacketInBlock;
                }
              }
            } catch (InterruptedException ine) {
              isInterrupted = true;
            } catch (IOException ioe) {
              if (Thread.interrupted()) {
                isInterrupted = true;
              } else {
                // continue to run even if can not read from mirror
                // notify client of the error
                // and wait for the client to shut down the pipeline
                mirrorError = true;
                LOG.info("PacketResponder " + block + " " + numTargets + 
                      " Exception " + StringUtils.stringifyException(ioe));
              }
            }

            if (Thread.interrupted() || isInterrupted) {
              /* The receiver thread cancelled this thread. 
               * We could also check any other status updates from the 
               * receiver thread (e.g. if it is ok to write to replyOut). 
               * It is prudent to not send any more status back to the client
               * because this datanode has a problem. The upstream datanode
               * will detect that this datanode is bad, and rightly so.
               */
              LOG.info("PacketResponder " + block +  " " + numTargets +
                       " : Thread is interrupted.");
              running = false;
              continue;
            }
            
            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (lastPacketInBlock) {
              receiver.close();
              final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
              block.setNumBytes(replicaInfo.getNumBytes());
              datanode.data.finalizeBlock(block);
              datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT);
              if (ClientTraceLog.isInfoEnabled() &&
                  receiver.clientName.length() > 0) {
                long offset = 0;
                ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT,
                      receiver.inAddr, receiver.myAddr, block.getNumBytes(),
                      "HDFS_WRITE", receiver.clientName, offset,
                      datanode.dnRegistration.getStorageID(), block, endTime-startTime));
              } else {
                LOG.info("Received block " + block + 
                         " of size " + block.getNumBytes() + 
                         " from " + receiver.inAddr);
              }
            }

            // construct my ack message
            Status[] replies = null;
            if (mirrorError) { // ack read error
              replies = new Status[2];
              replies[0] = SUCCESS;
              replies[1] = ERROR;
            } else {
              short ackLen = numTargets == 0 ? 0 : ack.getNumOfReplies();
              replies = new Status[1+ackLen];
              replies[0] = SUCCESS;
              for (int i=0; i<ackLen; i++) {
                replies[i+1] = ack.getReply(i);
              }
            }
            PipelineAck replyAck = new PipelineAck(expected, replies);
            
            // send my ack back to upstream datanode
            replyAck.write(replyOut);
            replyOut.flush();
            if (LOG.isDebugEnabled()) {
              LOG.debug("PacketResponder " + numTargets + 
                        " for block " + block +
                        " responded an ack: " + replyAck);
            }
            if (pkt != null) {
              // remove the packet from the ack queue
              removeAckHead();
              // update bytes acked
              if (replyAck.isSuccess() && 
                  pkt.lastByteInBlock>replicaInfo.getBytesAcked()) {
                replicaInfo.setBytesAcked(pkt.lastByteInBlock);
              }
            }
        } catch (IOException e) {
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            try {
              datanode.checkDiskError(e); // may throw an exception here
            } catch (IOException ioe) {
              LOG.warn("DataNode.chekDiskError failed in run() with: ", ioe);
            }
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt();
            }
          }
        } catch (Throwable e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
            receiverThread.interrupt();
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }
    
    /**
     * Remove a packet from the head of the ack queue
     * 
     * This should be called only when the ack queue is not empty
     */
    private synchronized void removeAckHead() {
      ackQueue.removeFirst();
      notifyAll();
    }
  }
  
  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  static private class Packet {
    long seqno;
    boolean lastPacketInBlock;
    long lastByteInBlock;

    Packet(long seqno, boolean lastPacketInBlock, long lastByteInPacket) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.lastByteInBlock = lastByteInPacket;
    }
  }
}
