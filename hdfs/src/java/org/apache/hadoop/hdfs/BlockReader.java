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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.CHECKSUM_OK;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.security.InvalidAccessTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;

/** This is a wrapper around connection to datadone
 * and understands checksum, offset etc
 */
@InterfaceAudience.Private
public class BlockReader extends FSInputChecker {

  Socket dnSock; //for now just sending checksumOk.
  private DataInputStream in;
  private DataChecksum checksum;

  /** offset in block of the last chunk received */
  private long lastChunkOffset = -1;
  private long lastChunkLen = -1;
  private long lastSeqNo = -1;

  /** offset in block where reader wants to actually read */
  private long startOffset;

  /** offset in block of of first chunk - may be less than startOffset
      if startOffset is not chunk-aligned */
  private final long firstChunkOffset;

  private int bytesPerChecksum;
  private int checksumSize;

  /**
   * The total number of bytes we need to transfer from the DN.
   * This is the amount that the user has requested plus some padding
   * at the beginning so that the read can begin on a chunk boundary.
   */
  private final long bytesNeededToFinish;

  private boolean gotEOS = false;
  
  byte[] skipBuf = null;
  ByteBuffer checksumBytes = null;
  int dataLeft = 0;
  
  /* FSInputChecker interface */
  
  /* same interface as inputStream java.io.InputStream#read()
   * used by DFSInputStream#read()
   * This violates one rule when there is a checksum error:
   * "Read should not modify user buffer before successful read"
   * because it first reads the data to user buffer and then checks
   * the checksum.
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len) 
                               throws IOException {
    
    // This has to be set here, *before* the skip, since we can
    // hit EOS during the skip, in the case that our entire read
    // is smaller than the checksum chunk.
    boolean eosBefore = gotEOS;

    //for the first read, skip the extra bytes at the front.
    if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
      // Skip these bytes. But don't call this.skip()!
      int toSkip = (int)(startOffset - firstChunkOffset);
      if ( skipBuf == null ) {
        skipBuf = new byte[bytesPerChecksum];
      }
      if ( super.read(skipBuf, 0, toSkip) != toSkip ) {
        // should never happen
        throw new IOException("Could not skip required number of bytes");
      }
    }
    
    int nRead = super.read(buf, off, len);
    
    // if gotEOS was set in the previous read and checksum is enabled :
    if (gotEOS && !eosBefore && nRead >= 0 && needChecksum()) {
      //checksum is verified and there are no errors.
      checksumOk(dnSock);
    }
    return nRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    /* How can we make sure we don't throw a ChecksumException, at least
     * in majority of the cases?. This one throws. */  
    if ( skipBuf == null ) {
      skipBuf = new byte[bytesPerChecksum]; 
    }

    long nSkipped = 0;
    while ( nSkipped < n ) {
      int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
      int ret = read(skipBuf, 0, toSkip);
      if ( ret <= 0 ) {
        return nSkipped;
      }
      nSkipped += ret;
    }
    return nSkipped;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("read() is not expected to be invoked. " +
                          "Use read(buf, off, len) instead.");
  }
  
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    /* Checksum errors are handled outside the BlockReader. 
     * DFSInputStream does not always call 'seekToNewSource'. In the 
     * case of pread(), it just tries a different replica without seeking.
     */ 
    return false;
  }
  
  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("Seek() is not supported in BlockInputChecker");
  }

  @Override
  protected long getChunkPosition(long pos) {
    throw new RuntimeException("getChunkPosition() is not supported, " +
                               "since seek is not required");
  }
  
  /**
   * Makes sure that checksumBytes has enough capacity 
   * and limit is set to the number of checksum bytes needed 
   * to be read.
   */
  private void adjustChecksumBytes(int dataLen) {
    int requiredSize = 
      ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize;
    if (checksumBytes == null || requiredSize > checksumBytes.capacity()) {
      checksumBytes =  ByteBuffer.wrap(new byte[requiredSize]);
    } else {
      checksumBytes.clear();
    }
    checksumBytes.limit(requiredSize);
  }
  
  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset, 
                                       int len, byte[] checksumBuf) 
                                       throws IOException {
    // Read one chunk.
    if ( gotEOS ) {
      // Already hit EOF
      return -1;
    }
    
    // Read one DATA_CHUNK.
    long chunkOffset = lastChunkOffset;
    if ( lastChunkLen > 0 ) {
      chunkOffset += lastChunkLen;
    }
    
    // pos is relative to the start of the first chunk of the read.
    // chunkOffset is relative to the start of the block.
    // This makes sure that the read passed from FSInputChecker is the
    // for the same chunk we expect to be reading from the DN.
    if ( (pos + firstChunkOffset) != chunkOffset ) {
      throw new IOException("Mismatch in pos : " + pos + " + " + 
                            firstChunkOffset + " != " + chunkOffset);
    }

    // Read next packet if the previous packet has been read completely.
    if (dataLeft <= 0) {
      //Read packet headers.
      int packetLen = in.readInt();
      long offsetInBlock = in.readLong();
      long seqno = in.readLong();
      boolean lastPacketInBlock = in.readBoolean();
    
      if (LOG.isDebugEnabled()) {
        LOG.debug("DFSClient readChunk got seqno " + seqno +
                  " offsetInBlock " + offsetInBlock +
                  " lastPacketInBlock " + lastPacketInBlock +
                  " packetLen " + packetLen);
      }
      
      int dataLen = in.readInt();
    
      // Sanity check the lengths
      if ( ( dataLen <= 0 && !lastPacketInBlock ) ||
           ( dataLen != 0 && lastPacketInBlock) ||
           (seqno != (lastSeqNo + 1)) ) {
           throw new IOException("BlockReader: error in packet header" +
                                 "(chunkOffset : " + chunkOffset + 
                                 ", dataLen : " + dataLen +
                                 ", seqno : " + seqno + 
                                 " (last: " + lastSeqNo + "))");
      }
      
      lastSeqNo = seqno;
      dataLeft = dataLen;
      adjustChecksumBytes(dataLen);
      if (dataLen > 0) {
        IOUtils.readFully(in, checksumBytes.array(), 0,
                          checksumBytes.limit());
      }
    }

    // Sanity checks
    assert len >= bytesPerChecksum;
    assert checksum != null;
    assert checksumSize == 0 || (checksumBuf.length % checksumSize == 0);


    int checksumsToRead, bytesToRead;

    if (checksumSize > 0) {

      // How many chunks left in our stream - this is a ceiling
      // since we may have a partial chunk at the end of the file
      int chunksLeft = (dataLeft - 1) / bytesPerChecksum + 1;

      // How many chunks we can fit in databuffer
      //  - note this is a floor since we always read full chunks
      int chunksCanFit = Math.min(len / bytesPerChecksum,
                                  checksumBuf.length / checksumSize);

      // How many chunks should we read
      checksumsToRead = Math.min(chunksLeft, chunksCanFit);
      // How many bytes should we actually read
      bytesToRead = Math.min(
        checksumsToRead * bytesPerChecksum, // full chunks
        dataLeft); // in case we have a partial
    } else {
      // no checksum
      bytesToRead = Math.min(dataLeft, len);
      checksumsToRead = 0;
    }

    if ( bytesToRead > 0 ) {
      // Assert we have enough space
      assert bytesToRead <= len;
      assert checksumBytes.remaining() >= checksumSize * checksumsToRead;
      assert checksumBuf.length >= checksumSize * checksumsToRead;
      IOUtils.readFully(in, buf, offset, bytesToRead);
      checksumBytes.get(checksumBuf, 0, checksumSize * checksumsToRead);
    }

    dataLeft -= bytesToRead;
    assert dataLeft >= 0;

    lastChunkOffset = chunkOffset;
    lastChunkLen = bytesToRead;

    // If there's no data left in the current packet after satisfying
    // this read, and we have satisfied the client read, we expect
    // an empty packet header from the DN to signify this.
    // Note that pos + bytesToRead may in fact be greater since the
    // DN finishes off the entire last chunk.
    if (dataLeft == 0 &&
        pos + bytesToRead >= bytesNeededToFinish) {

      // Read header
      int packetLen = in.readInt();
      long offsetInBlock = in.readLong();
      long seqno = in.readLong();
      boolean lastPacketInBlock = in.readBoolean();
      int dataLen = in.readInt();

      if (!lastPacketInBlock ||
          dataLen != 0) {
        throw new IOException("Expected empty end-of-read packet! Header: " +
                              "(packetLen : " + packetLen + 
                              ", offsetInBlock : " + offsetInBlock +
                              ", seqno : " + seqno + 
                              ", lastInBlock : " + lastPacketInBlock +
                              ", dataLen : " + dataLen);
      }

      gotEOS = true;
    }

    if ( bytesToRead == 0 ) {
      return -1;
    }

    return bytesToRead;
  }
  
  private BlockReader( String file, long blockId, DataInputStream in, 
                       DataChecksum checksum, boolean verifyChecksum,
                       long startOffset, long firstChunkOffset,
                       long bytesToRead,
                       Socket dnSock ) {
    super(new Path("/blk_" + blockId + ":of:" + file)/*too non path-like?*/,
          1, verifyChecksum,
          checksum.getChecksumSize() > 0? checksum : null, 
          checksum.getBytesPerChecksum(),
          checksum.getChecksumSize());
    
    this.dnSock = dnSock;
    this.in = in;
    this.checksum = checksum;
    this.startOffset = Math.max( startOffset, 0 );

    // The total number of bytes that we need to transfer from the DN is
    // the amount that the user wants (bytesToRead), plus the padding at
    // the beginning in order to chunk-align. Note that the DN may elect
    // to send more than this amount if the read ends mid-chunk.
    this.bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);

    this.firstChunkOffset = firstChunkOffset;
    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;

    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();
  }

  public static BlockReader newBlockReader(Socket sock, String file, long blockId, BlockAccessToken accessToken, 
      long genStamp, long startOffset, long len, int bufferSize) throws IOException {
    return newBlockReader(sock, file, blockId, accessToken, genStamp, startOffset, len, bufferSize,
        true);
  }

  /** Java Doc required */
  public static BlockReader newBlockReader( Socket sock, String file, long blockId, 
                                     BlockAccessToken accessToken,
                                     long genStamp,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum)
                                     throws IOException {
    return newBlockReader(sock, file, blockId, accessToken, genStamp, startOffset,
                          len, bufferSize, verifyChecksum, "");
  }

  public static BlockReader newBlockReader( Socket sock, String file,
                                     long blockId, 
                                     BlockAccessToken accessToken,
                                     long genStamp,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum,
                                     String clientName)
                                     throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    DataTransferProtocol.Sender.opReadBlock(
        new DataOutputStream(new BufferedOutputStream(
            NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT))),
        blockId, genStamp, startOffset, len, clientName, accessToken);
    
    //
    // Get bytes in block, set streams
    //

    DataInputStream in = new DataInputStream(
        new BufferedInputStream(NetUtils.getInputStream(sock), 
                                bufferSize));
    
    DataTransferProtocol.Status status = DataTransferProtocol.Status.read(in);
    if (status != SUCCESS) {
      if (status == ERROR_ACCESS_TOKEN) {
        throw new InvalidAccessTokenException(
            "Got access token error for OP_READ_BLOCK, self="
                + sock.getLocalSocketAddress() + ", remote="
                + sock.getRemoteSocketAddress() + ", for file " + file
                + ", for block " + blockId + "_" + genStamp);
      } else {
        throw new IOException("Got error for OP_READ_BLOCK, self="
            + sock.getLocalSocketAddress() + ", remote="
            + sock.getRemoteSocketAddress() + ", for file " + file
            + ", for block " + blockId + "_" + genStamp);
      }
    }
    DataChecksum checksum = DataChecksum.newDataChecksum( in );
    //Warning when we get CHECKSUM_NULL?
    
    // Read the first chunk offset.
    long firstChunkOffset = in.readLong();
    
    if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
        firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
      throw new IOException("BlockReader: error in first chunk offset (" +
                            firstChunkOffset + ") startOffset is " + 
                            startOffset + " for file " + file);
    }

    return new BlockReader( file, blockId, in, checksum, verifyChecksum,
                            startOffset, firstChunkOffset, len,
                            sock );
  }

  @Override
  public synchronized void close() throws IOException {
    startOffset = -1;
    checksum = null;
    // in will be closed when its Socket is closed.
  }
  
  /** kind of like readFully(). Only reads as much as possible.
   * And allows use of protected readFully().
   */
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return readFully(this, buf, offset, len);
  }
  
  /* When the reader reaches end of a block and there are no checksum
   * errors, we send OP_STATUS_CHECKSUM_OK to datanode to inform that 
   * checksum was verified and there was no error.
   */ 
  void checksumOk(Socket sock) {
    try {
      OutputStream out = NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT);
      CHECKSUM_OK.writeOutputStream(out);
      out.flush();
    } catch (IOException e) {
      // its ok not to be able to send this.
      LOG.debug("Could not write to datanode " + sock.getInetAddress() +
                ": " + e.getMessage());
    }
  }
}
