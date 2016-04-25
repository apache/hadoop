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
package org.apache.hadoop.hdfs.client.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.PeerCache;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @deprecated this is an old implementation that is being left around
 * in case any issues spring up with the new {@link BlockReaderRemote2}
 * implementation.
 * It will be removed in the next release.
 */
@InterfaceAudience.Private
@Deprecated
public class BlockReaderRemote extends FSInputChecker implements BlockReader {
  static final Logger LOG = LoggerFactory.getLogger(FSInputChecker.class);

  private final Peer peer;
  private final DatanodeID datanodeID;
  private final DataInputStream in;
  private DataChecksum checksum;

  /** offset in block of the last chunk received */
  private long lastChunkOffset = -1;
  private long lastChunkLen = -1;
  private long lastSeqNo = -1;

  /** offset in block where reader wants to actually read */
  private long startOffset;

  private final long blockId;

  /** offset in block of of first chunk - may be less than startOffset
   if startOffset is not chunk-aligned */
  private final long firstChunkOffset;

  private final int bytesPerChecksum;
  private final int checksumSize;

  /**
   * The total number of bytes we need to transfer from the DN.
   * This is the amount that the user has requested plus some padding
   * at the beginning so that the read can begin on a chunk boundary.
   */
  private final long bytesNeededToFinish;

  private boolean eos = false;
  private boolean sentStatusCode = false;

  ByteBuffer checksumBytes = null;
  /** Amount of unread data in the current received packet */
  int dataLeft = 0;

  private final PeerCache peerCache;

  private final Tracer tracer;

  private final int networkDistance;

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
    boolean eosBefore = eos;

    //for the first read, skip the extra bytes at the front.
    if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
      // Skip these bytes. But don't call this.skip()!
      int toSkip = (int)(startOffset - firstChunkOffset);
      if ( super.readAndDiscard(toSkip) != toSkip ) {
        // should never happen
        throw new IOException("Could not skip required number of bytes");
      }
    }

    int nRead = super.read(buf, off, len);

    // if eos was set in the previous read, send a status code to the DN
    if (eos && !eosBefore && nRead >= 0) {
      if (needChecksum()) {
        sendReadResult(peer, Status.CHECKSUM_OK);
      } else {
        sendReadResult(peer, Status.SUCCESS);
      }
    }
    return nRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    /* How can we make sure we don't throw a ChecksumException, at least
     * in majority of the cases?. This one throws. */
    long nSkipped = 0;
    while (nSkipped < n) {
      int toSkip = (int)Math.min(n-nSkipped, Integer.MAX_VALUE);
      int ret = readAndDiscard(toSkip);
      if (ret <= 0) {
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
    try (TraceScope ignored = tracer.newScope(
        "BlockReaderRemote#readChunk(" + blockId + ")")) {
      return readChunkImpl(pos, buf, offset, len, checksumBuf);
    }
  }

  private synchronized int readChunkImpl(long pos, byte[] buf, int offset,
      int len, byte[] checksumBuf)
      throws IOException {
    // Read one chunk.
    if (eos) {
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
      PacketHeader header = new PacketHeader();
      header.readFields(in);

      LOG.debug("DFSClient readChunk got header {}", header);

      // Sanity check the lengths
      if (!header.sanityCheck(lastSeqNo)) {
        throw new IOException("BlockReader: error in packet header " +
            header);
      }

      lastSeqNo = header.getSeqno();
      dataLeft = header.getDataLen();
      adjustChecksumBytes(header.getDataLen());
      if (header.getDataLen() > 0) {
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

      // How many chunks left in our packet - this is a ceiling
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
      PacketHeader hdr = new PacketHeader();
      hdr.readFields(in);

      if (!hdr.isLastPacketInBlock() ||
          hdr.getDataLen() != 0) {
        throw new IOException("Expected empty end-of-read packet! Header: " +
            hdr);
      }

      eos = true;
    }

    if ( bytesToRead == 0 ) {
      return -1;
    }

    return bytesToRead;
  }

  private BlockReaderRemote(String file, String bpid, long blockId,
      DataInputStream in, DataChecksum checksum, boolean verifyChecksum,
      long startOffset, long firstChunkOffset, long bytesToRead, Peer peer,
      DatanodeID datanodeID, PeerCache peerCache, Tracer tracer,
      int networkDistance) {
    // Path is used only for printing block and file information in debug
    super(new Path("/" + Block.BLOCK_FILE_PREFIX + blockId +
            ":" + bpid + ":of:"+ file)/*too non path-like?*/,
        1, verifyChecksum,
        checksum.getChecksumSize() > 0? checksum : null,
        checksum.getBytesPerChecksum(),
        checksum.getChecksumSize());

    this.peer = peer;
    this.datanodeID = datanodeID;
    this.in = in;
    this.checksum = checksum;
    this.startOffset = Math.max( startOffset, 0 );
    this.blockId = blockId;

    // The total number of bytes that we need to transfer from the DN is
    // the amount that the user wants (bytesToRead), plus the padding at
    // the beginning in order to chunk-align. Note that the DN may elect
    // to send more than this amount if the read starts/ends mid-chunk.
    this.bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);

    this.firstChunkOffset = firstChunkOffset;
    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;

    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();
    this.peerCache = peerCache;
    this.tracer = tracer;
    this.networkDistance = networkDistance;
  }

  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_READ_BLOCK request.
   *
   * @param file  File location
   * @param block  The block object
   * @param blockToken  The block token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name
   * @return New BlockReader instance, or null on error.
   */
  public static BlockReaderRemote newBlockReader(String file,
      ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken,
      long startOffset, long len,
      int bufferSize, boolean verifyChecksum,
      String clientName, Peer peer,
      DatanodeID datanodeID,
      PeerCache peerCache,
      CachingStrategy cachingStrategy,
      Tracer tracer, int networkDistance)
      throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(peer.getOutputStream()));
    new Sender(out).readBlock(block, blockToken, clientName, startOffset, len,
        verifyChecksum, cachingStrategy);

    //
    // Get bytes in block, set streams
    //

    DataInputStream in = new DataInputStream(
        new BufferedInputStream(peer.getInputStream(), bufferSize));

    BlockOpResponseProto status = BlockOpResponseProto.parseFrom(
        PBHelperClient.vintPrefixed(in));
    BlockReaderRemote2.checkSuccess(status, peer, block, file);
    ReadOpChecksumInfoProto checksumInfo =
        status.getReadOpChecksumInfo();
    DataChecksum checksum = DataTransferProtoUtil.fromProto(
        checksumInfo.getChecksum());
    //Warning when we get CHECKSUM_NULL?

    // Read the first chunk offset.
    long firstChunkOffset = checksumInfo.getChunkOffset();

    if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
        firstChunkOffset <= (startOffset - checksum.getBytesPerChecksum())) {
      throw new IOException("BlockReader: error in first chunk offset (" +
          firstChunkOffset + ") startOffset is " +
          startOffset + " for file " + file);
    }

    return new BlockReaderRemote(file, block.getBlockPoolId(), block.getBlockId(),
        in, checksum, verifyChecksum, startOffset, firstChunkOffset, len,
        peer, datanodeID, peerCache, tracer, networkDistance);
  }

  @Override
  public synchronized void close() throws IOException {
    startOffset = -1;
    checksum = null;
    if (peerCache != null & sentStatusCode) {
      peerCache.put(datanodeID, peer);
    } else {
      peer.close();
    }

    // in will be closed when its Socket is closed.
  }

  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead)
      throws IOException {
    IOUtils.readFully(this, buf, readOffset, amtToRead);
  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return readFully(this, buf, offset, len);
  }

  /**
   * When the reader reaches end of the read, it sends a status response
   * (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
   * closing our connection (which we will re-open), but won't affect
   * data correctness.
   */
  void sendReadResult(Peer peer, Status statusCode) {
    assert !sentStatusCode : "already sent status code to " + peer;
    try {
      BlockReaderRemote2.writeReadResult(peer.getOutputStream(), statusCode);
      sentStatusCode = true;
    } catch (IOException e) {
      // It's ok not to be able to send this. But something is probably wrong.
      LOG.info("Could not send read status (" + statusCode + ") to datanode " +
          peer.getRemoteAddressString() + ": " + e.getMessage());
    }
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    throw new UnsupportedOperationException("readDirect unsupported in BlockReaderRemote");
  }

  @Override
  public int available() {
    // An optimistic estimate of how much data is available
    // to us without doing network I/O.
    return BlockReaderRemote2.TCP_WINDOW_SIZE;
  }

  @Override
  public boolean isShortCircuit() {
    return false;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }

  @Override
  public DataChecksum getDataChecksum() {
    return checksum;
  }

  @Override
  public int getNetworkDistance() {
    return networkDistance;
  }
}
