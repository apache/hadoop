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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/**
 * Reads a block from the disk and sends it to a recipient.
 */
class BlockSender implements java.io.Closeable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private Block block; // the block to read from

  /** the replica to read from */
  private final Replica replica;
  /** The visible length of a replica. */
  private final long replicaVisibleLength;

  private InputStream blockIn; // data stream
  private long blockInPosition = -1; // updated while using transferTo().
  private DataInputStream checksumIn; // checksum datastream
  private DataChecksum checksum; // checksum stream
  private long offset; // starting position to read
  private long endOffset; // ending position
  private int bytesPerChecksum; // chunk size
  private int checksumSize; // checksum size
  private boolean corruptChecksumOk; // if need to verify checksum
  private boolean chunkOffsetOK; // if need to send chunk offset
  private long seqno; // sequence number of packet

  private boolean transferToAllowed = true;
  private boolean blockReadFully; //set when the whole block is read
  private boolean verifyChecksum; //if true, check is verified while reading
  private BlockTransferThrottler throttler;
  private final String clientTraceFmt; // format of client trace log message

  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private volatile ChunkChecksum lastChunkChecksum = null;

  
  BlockSender(Block block, long startOffset, long length,
              boolean corruptChecksumOk, boolean chunkOffsetOK,
              boolean verifyChecksum, DataNode datanode) throws IOException {
    this(block, startOffset, length, corruptChecksumOk, chunkOffsetOK,
         verifyChecksum, datanode, null);
  }

  BlockSender(Block block, long startOffset, long length,
              boolean corruptChecksumOk, boolean chunkOffsetOK,
              boolean verifyChecksum, DataNode datanode, String clientTraceFmt)
      throws IOException {
    try {
      this.block = block;
      synchronized(datanode.data) { 
        this.replica = datanode.data.getReplica(block.getBlockId());
        if (replica == null) {
          throw new ReplicaNotFoundException(block);
        }
        this.replicaVisibleLength = replica.getVisibleLength();
      }
      long minEndOffset = startOffset + length;
      // if this is a write in progress
      ChunkChecksum chunkChecksum = null;
      if (replica instanceof ReplicaBeingWritten) {
        for (int i = 0; i < 30 && replica.getBytesOnDisk() < minEndOffset; i++) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            throw new IOException(ie);
          }
        }

        long currentBytesOnDisk = replica.getBytesOnDisk();
        
        if (currentBytesOnDisk < minEndOffset) {
          throw new IOException(String.format(
            "need %d bytes, but only %d bytes available",
            minEndOffset,
            currentBytesOnDisk
          ));
        }

        ReplicaInPipeline rip = (ReplicaInPipeline) replica;
        chunkChecksum = rip.getLastChecksumAndDataLen();
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException(
            "replica.getGenerationStamp() < block.getGenerationStamp(), block="
            + block + ", replica=" + replica);
      }
      if (replicaVisibleLength < 0) {
        throw new IOException("The replica is not readable, block="
            + block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }
      
      this.chunkOffsetOK = chunkOffsetOK;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;
      this.transferToAllowed = datanode.transferToAllowed;
      this.clientTraceFmt = clientTraceFmt;

      if ( !corruptChecksumOk || datanode.data.metaFileExists(block) ) {
        checksumIn = new DataInputStream(
                new BufferedInputStream(datanode.data.getMetaDataInputStream(block),
                                        BUFFER_SIZE));

        // read and handle the common header here. For now just a version
       BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
       short version = header.getVersion();

        if (version != FSDataset.METADATA_VERSION) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + block + " ignoring ...");
        }
        checksum = header.getChecksum();
      } else {
        LOG.warn("Could not find metadata file for " + block);
        // This only decides the buffer size. Use BUFFER_SIZE?
        checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL,
            16 * 1024);
      }

      /* If bytesPerChecksum is very large, then the metadata file
       * is mostly corrupted. For now just truncate bytesPerchecksum to
       * blockLength.
       */        
      bytesPerChecksum = checksum.getBytesPerChecksum();
      if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > replicaVisibleLength) {
        checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
            Math.max((int)replicaVisibleLength, 10*1024*1024));
        bytesPerChecksum = checksum.getBytesPerChecksum();        
      }
      checksumSize = checksum.getChecksumSize();

      if (length < 0) {
        length = replicaVisibleLength;
      }

      // end is either last byte on disk or the length for which we have a 
      // checksum
      if (chunkChecksum != null) {
        endOffset = chunkChecksum.getDataLength();
      } else {
        endOffset = replica.getBytesOnDisk();
      }
      
      if (startOffset < 0 || startOffset > endOffset
          || (length + startOffset) > endOffset) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + endOffset + " )";
        LOG.warn(datanode.dnRegistration + ":sendBlock() : " + msg);
        throw new IOException(msg);
      }

      
      offset = (startOffset - (startOffset % bytesPerChecksum));
      if (length >= 0) {
        // Make sure endOffset points to end of a checksumed chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % bytesPerChecksum != 0) {
          tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
        }
        if (tmpLen < endOffset) {
          // will use on-disk checksum here since the end is a stable chunk
          endOffset = tmpLen;
        } else if (chunkChecksum != null) {
          //in last chunk which is changing. flag that we need to use in-memory 
          // checksum 
          this.lastChunkChecksum = chunkChecksum;
        }
      }

      // seek to the right offsets
      if (offset > 0) {
        long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
        // note blockInStream is  seeked when created below
        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      seqno = 0;

      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("replica=" + replica);
      }

      blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      IOUtils.closeStream(blockIn);
      throw ioe;
    }
  }

  /**
   * close opened files.
   */
  public void close() throws IOException {
    IOException ioe = null;
    // close checksum file
    if(checksumIn!=null) {
      try {
        checksumIn.close();
      } catch (IOException e) {
        ioe = e;
      }
      checksumIn = null;
    }
    // close data file
    if(blockIn!=null) {
      try {
        blockIn.close();
      } catch (IOException e) {
        ioe = e;
      }
      blockIn = null;
    }
    // throw IOException if there is any
    if(ioe!= null) {
      throw ioe;
    }
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException.
   * This is typically done to indicate to upper layers that the error 
   * was a socket error rather than often more serious exceptions like 
   * disk errors.
   */
  private static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /* Change the stacktrace so that original trace is not truncated
       * when printed.*/ 
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }

  /**
   * Sends upto maxChunks chunks of data.
   * 
   * When blockInPosition is >= 0, assumes 'out' is a 
   * {@link SocketOutputStream} and tries 
   * {@link SocketOutputStream#transferToFully(FileChannel, long, int)} to
   * send data (and updates blockInPosition).
   */
  private int sendChunks(ByteBuffer pkt, int maxChunks, OutputStream out) 
                         throws IOException {
    // Sends multiple chunks in one packet with a single write().

    int len = Math.min((int) (endOffset - offset),
                       bytesPerChecksum*maxChunks);
    int numChunks = (len + bytesPerChecksum - 1)/bytesPerChecksum;
    int packetLen = len + numChunks*checksumSize + 4;
    boolean lastDataPacket = offset + len == endOffset && len > 0;
    pkt.clear();
    
    // write packet header
    pkt.putInt(packetLen);
    pkt.putLong(offset);
    pkt.putLong(seqno);
    pkt.put((byte)((len == 0) ? 1 : 0));
               //why no ByteBuf.putBoolean()?
    pkt.putInt(len);
    
    int checksumOff = pkt.position();
    int checksumLen = numChunks * checksumSize;
    byte[] buf = pkt.array();
    
    if (checksumSize > 0 && checksumIn != null) {
      try {
        checksumIn.readFully(buf, checksumOff, checksumLen);
      } catch (IOException e) {
        LOG.warn(" Could not read or failed to veirfy checksum for data" +
                 " at offset " + offset + " for block " + block + " got : "
                 + StringUtils.stringifyException(e));
        IOUtils.closeStream(checksumIn);
        checksumIn = null;
        if (corruptChecksumOk) {
          if (checksumOff < checksumLen) {
            // Just fill the array with zeros.
            Arrays.fill(buf, checksumOff, checksumLen, (byte) 0);
          }
        } else {
          throw e;
        }
      }

      // write in progress that we need to use to get last checksum
      if (lastDataPacket && lastChunkChecksum != null) {
        int start = checksumOff + checksumLen - checksumSize;
        byte[] updatedChecksum = lastChunkChecksum.getChecksum();
        
        if (updatedChecksum != null) {
          System.arraycopy(updatedChecksum, 0, buf, start, checksumSize);
        }
      }
    }
    
    int dataOff = checksumOff + checksumLen;
    
    if (blockInPosition < 0) {
      //normal transfer
      IOUtils.readFully(blockIn, buf, dataOff, len);

      if (verifyChecksum) {
        int dOff = dataOff;
        int cOff = checksumOff;
        int dLeft = len;

        for (int i=0; i<numChunks; i++) {
          checksum.reset();
          int dLen = Math.min(dLeft, bytesPerChecksum);
          checksum.update(buf, dOff, dLen);
          if (!checksum.compare(buf, cOff)) {
            long failedPos = offset + len -dLeft;
            throw new ChecksumException("Checksum failed at " + 
                                        failedPos, failedPos);
          }
          dLeft -= dLen;
          dOff += dLen;
          cOff += checksumSize;
        }
      }
      //writing is done below (mainly to handle IOException)
    }
    
    try {
      if (blockInPosition >= 0) {
        //use transferTo(). Checks on out and blockIn are already done. 

        SocketOutputStream sockOut = (SocketOutputStream)out;
        //first write the packet
        sockOut.write(buf, 0, dataOff);
        // no need to flush. since we know out is not a buffered stream. 

        sockOut.transferToFully(((FileInputStream)blockIn).getChannel(), 
                                blockInPosition, len);

        blockInPosition += len;
      } else {
        // normal transfer
        out.write(buf, 0, dataOff + len);
      }
      
    } catch (IOException e) {
      /* exception while writing to the client (well, with transferTo(),
       * it could also be while reading from the local file).
       */
      throw ioeToSocketException(e);
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return len;
  }

  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @return total bytes reads, including crc.
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 BlockTransferThrottler throttler) throws IOException {
    if( out == null ) {
      throw new IOException( "out stream is null" );
    }
    this.throttler = throttler;

    long initialOffset = offset;
    long totalRead = 0;
    OutputStream streamForSendChunks = out;
    
    final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
    try {
      try {
        checksum.writeHeader(out);
        if ( chunkOffsetOK ) {
          out.writeLong( offset );
        }
        out.flush();
      } catch (IOException e) { //socket error
        throw ioeToSocketException(e);
      }
      
      int maxChunksPerPacket;
      int pktSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
      
      if (transferToAllowed && !verifyChecksum && 
          baseStream instanceof SocketOutputStream && 
          blockIn instanceof FileInputStream) {
        
        FileChannel fileChannel = ((FileInputStream)blockIn).getChannel();
        
        // blockInPosition also indicates sendChunks() uses transferTo.
        blockInPosition = fileChannel.position();
        streamForSendChunks = baseStream;
        
        // assure a mininum buffer size.
        maxChunksPerPacket = (Math.max(BUFFER_SIZE, 
                                       MIN_BUFFER_WITH_TRANSFERTO)
                              + bytesPerChecksum - 1)/bytesPerChecksum;
        
        // allocate smaller buffer while using transferTo(). 
        pktSize += checksumSize * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1,
                 (BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
        pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
      }

      ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

      while (endOffset > offset) {
        long len = sendChunks(pktBuf, maxChunksPerPacket, 
                              streamForSendChunks);
        offset += len;
        totalRead += len + ((len + bytesPerChecksum - 1)/bytesPerChecksum*
                            checksumSize);
        seqno++;
      }
      try {
        // send an empty packet to mark the end of the block
        sendChunks(pktBuf, maxChunksPerPacket, streamForSendChunks);        
        out.flush();
      } catch (IOException e) { //socket error
        throw ioeToSocketException(e);
      }
    } finally {
      if (clientTraceFmt != null) {
        final long endTime = System.nanoTime();
        ClientTraceLog.info(String.format(clientTraceFmt, totalRead, initialOffset, endTime - startTime));
      }
      close();
    }

    blockReadFully = initialOffset == 0 && offset >= replicaVisibleLength;

    return totalRead;
  }
  
  boolean isBlockReadFully() {
    return blockReadFully;
  }
}
