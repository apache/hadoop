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

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumMd5CrcReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedReconstructionInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for Block checksum computing, for both replicated and striped
 * blocks.
 */
@InterfaceAudience.Private
final class BlockChecksumHelper {

  static final Logger LOG = LoggerFactory.getLogger(BlockChecksumHelper.class);

  private BlockChecksumHelper() {
  }

  /**
   * The abstract block checksum computer.
   */
  static abstract class AbstractBlockChecksumComputer {
    private final DataNode datanode;
    private final BlockChecksumOptions blockChecksumOptions;

    private byte[] outBytes;
    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = null;
    private long crcPerBlock = -1;
    private int checksumSize = -1;

    AbstractBlockChecksumComputer(
        DataNode datanode,
        BlockChecksumOptions blockChecksumOptions) throws IOException {
      this.datanode = datanode;
      this.blockChecksumOptions = blockChecksumOptions;
    }

    abstract void compute() throws IOException;

    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }

    DataNode getDatanode() {
      return datanode;
    }

    BlockChecksumOptions getBlockChecksumOptions() {
      return blockChecksumOptions;
    }

    InputStream getBlockInputStream(ExtendedBlock block, long seekOffset)
        throws IOException {
      return datanode.data.getBlockInputStream(block, seekOffset);
    }

    void setOutBytes(byte[] bytes) {
      this.outBytes = bytes;
    }

    byte[] getOutBytes() {
      return outBytes;
    }

    int getBytesPerCRC() {
      return bytesPerCRC;
    }

    public void setBytesPerCRC(int bytesPerCRC) {
      this.bytesPerCRC = bytesPerCRC;
    }

    public void setCrcType(DataChecksum.Type crcType) {
      this.crcType = crcType;
    }

    public void setCrcPerBlock(long crcPerBlock) {
      this.crcPerBlock = crcPerBlock;
    }

    public void setChecksumSize(int checksumSize) {
      this.checksumSize = checksumSize;
    }

    DataChecksum.Type getCrcType() {
      return crcType;
    }

    long getCrcPerBlock() {
      return crcPerBlock;
    }

    int getChecksumSize() {
      return checksumSize;
    }
  }

  /**
   * The abstract base block checksum computer, mainly for replicated blocks.
   */
  static abstract class BlockChecksumComputer
      extends AbstractBlockChecksumComputer {
    private final ExtendedBlock block;
    // client side now can specify a range of the block for checksum
    private final long requestLength;
    private final LengthInputStream metadataIn;
    private final DataInputStream checksumIn;
    private final long visibleLength;
    private final boolean partialBlk;

    private BlockMetadataHeader header;
    private DataChecksum checksum;

    BlockChecksumComputer(DataNode datanode,
                          ExtendedBlock block,
                          BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, blockChecksumOptions);
      this.block = block;
      this.requestLength = block.getNumBytes();
      Preconditions.checkArgument(requestLength >= 0);

      this.metadataIn = datanode.data.getMetaDataInputStream(block);
      this.visibleLength = datanode.data.getReplicaVisibleLength(block);
      this.partialBlk = requestLength < visibleLength;

      int ioFileBufferSize =
          DFSUtilClient.getIoFileBufferSize(datanode.getConf());
      this.checksumIn = new DataInputStream(
          new BufferedInputStream(metadataIn, ioFileBufferSize));
    }

    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }


    ExtendedBlock getBlock() {
      return block;
    }

    long getRequestLength() {
      return requestLength;
    }

    LengthInputStream getMetadataIn() {
      return metadataIn;
    }

    DataInputStream getChecksumIn() {
      return checksumIn;
    }

    long getVisibleLength() {
      return visibleLength;
    }

    boolean isPartialBlk() {
      return partialBlk;
    }

    BlockMetadataHeader getHeader() {
      return header;
    }

    DataChecksum getChecksum() {
      return checksum;
    }

    /**
     * Perform the block checksum computing.
     *
     * @throws IOException
     */
    abstract void compute() throws IOException;

    /**
     * Read block metadata header.
     *
     * @throws IOException
     */
    void readHeader() throws IOException {
      //read metadata file
      header = BlockMetadataHeader.readHeader(checksumIn);
      checksum = header.getChecksum();
      setChecksumSize(checksum.getChecksumSize());
      setBytesPerCRC(checksum.getBytesPerChecksum());
      long crcPerBlock = checksum.getChecksumSize() <= 0 ? 0 :
          (metadataIn.getLength() -
              BlockMetadataHeader.getHeaderSize()) / checksum.getChecksumSize();
      setCrcPerBlock(crcPerBlock);
      setCrcType(checksum.getChecksumType());
    }

    /**
     * Calculate partial block checksum.
     *
     * @return
     * @throws IOException
     */
    byte[] crcPartialBlock() throws IOException {
      int partialLength = (int) (requestLength % getBytesPerCRC());
      if (partialLength > 0) {
        byte[] buf = new byte[partialLength];
        final InputStream blockIn = getBlockInputStream(block,
            requestLength - partialLength);
        try {
          // Get the CRC of the partialLength.
          IOUtils.readFully(blockIn, buf, 0, partialLength);
        } finally {
          IOUtils.closeStream(blockIn);
        }
        checksum.update(buf, 0, partialLength);
        byte[] partialCrc = new byte[getChecksumSize()];
        checksum.writeValue(partialCrc, 0, true);
        return partialCrc;
      }

      return null;
    }
  }

  /**
   * Replicated block checksum computer.
   */
  static class ReplicatedBlockChecksumComputer extends BlockChecksumComputer {

    ReplicatedBlockChecksumComputer(DataNode datanode,
                                    ExtendedBlock block,
                                    BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, block, blockChecksumOptions);
    }

    @Override
    void compute() throws IOException {
      try {
        readHeader();

        BlockChecksumType type =
            getBlockChecksumOptions().getBlockChecksumType();
        switch (type) {
          case MD5CRC:
            computeMd5Crc();
            break;
          case COMPOSITE_CRC:
            computeCompositeCrc(0);
            break;
          case STRIPED_CRC:
            computeCompositeCrc(getBlockChecksumOptions().getStripeLength());
            break;
          default:
            throw new IOException(String.format(
                "Unrecognized BlockChecksumType: %s", type));
        }
      } finally {
        IOUtils.closeStream(getChecksumIn());
        IOUtils.closeStream(getMetadataIn());
      }
    }

    private void computeMd5Crc() throws IOException {
      MD5Hash md5out;
      if (isPartialBlk() && getCrcPerBlock() > 0) {
        md5out = checksumPartialBlock();
      } else {
        md5out = checksumWholeBlock();
      }
      setOutBytes(md5out.getDigest());

      LOG.debug("block={}, bytesPerCRC={}, crcPerBlock={}, md5out={}",
          getBlock(), getBytesPerCRC(), getCrcPerBlock(), md5out);
    }

    private MD5Hash checksumWholeBlock() throws IOException {
      MD5Hash md5out = MD5Hash.digest(getChecksumIn());
      return md5out;
    }

    private MD5Hash checksumPartialBlock() throws IOException {
      byte[] buffer = new byte[4 * 1024];
      MessageDigest digester = MD5Hash.getDigester();

      long remaining = (getRequestLength() / getBytesPerCRC())
          * getChecksumSize();
      for (int toDigest = 0; remaining > 0; remaining -= toDigest) {
        toDigest = getChecksumIn().read(buffer, 0,
            (int) Math.min(remaining, buffer.length));
        if (toDigest < 0) {
          break;
        }
        digester.update(buffer, 0, toDigest);
      }

      byte[] partialCrc = crcPartialBlock();
      if (partialCrc != null) {
        digester.update(partialCrc);
      }

      return new MD5Hash(digester.digest());
    }

    private void computeCompositeCrc(long stripeLength) throws IOException {
      long checksumDataLength =
          Math.min(getVisibleLength(), getRequestLength());
      if (stripeLength <= 0 || stripeLength > checksumDataLength) {
        stripeLength = checksumDataLength;
      }
      
      // TODO(dhuo): Check bounds; we can't support numStripes >
      // Integer.MAX_VALUE with this approach because we buffer
      // all striped CRCs in memory before composing them in correct logical
      // order. Alternatively we'd either need to spill to disk or pass
      // around a running composite.
      int numStripes = (int) (checksumDataLength / stripeLength);
      if (checksumDataLength % stripeLength != 0) {
        ++numStripes;
      }
      long crcsPerStripe;
      if (stripeLength == checksumDataLength) {
        crcsPerStripe = Long.MAX_VALUE;
      } else if (stripeLength % getBytesPerCRC() == 0) {
        crcsPerStripe = stripeLength / getBytesPerCRC();
      } else {
        throw new IOException(String.format(
            "stripeLength(=%d) must either be full data length (=%d) or "
            + "divisible by bytesPerCrc(=%d)",
            stripeLength, checksumDataLength, getBytesPerCRC()));
      }
      LOG.debug("computeCompositeCrc stripeLength={}, checksumDataLength={}, "
          + "numStripes={}, crcsPerStripe={}, getBytesPerCRC()={}",
          stripeLength, checksumDataLength, numStripes, crcsPerStripe,
          getBytesPerCRC());

      // TODO(dhuo): maybe use DataOutputBuffer for this instead, but remember
      // to deal with the fact that its getData() returns a powers-of-two
      // buffer larger than actual written data.
      byte[] stripedCrcs = new byte[4 * numStripes];
      int stripedCrcsCursor = 0;  // Index into stripedCrcs.

      int compositeCrc = 0;
      int crcPolynomial = DataChecksum.getCrcPolynomialForType(getCrcType());
      DataInputStream checksumIn = getChecksumIn();

      // Whether getting the checksum for the entire block (which itself may
      // not be a full block size and may have a final chunk smaller than
      // getBytesPerCRC()), we begin with a number of full chunks, all of size
      // getBytesPerCRC().
      long numFullChunks = checksumDataLength / getBytesPerCRC();
      int fullChunkMonomial = CrcUtil.getMonomial(getBytesPerCRC(), crcPolynomial);
      LOG.debug(String.format(
          "Using monomial 0x%08x for length %d and CRC polynomial 0x%08x",
          fullChunkMonomial, getBytesPerCRC(), crcPolynomial));
      for (long chunkNum = 0; chunkNum < numFullChunks; ++chunkNum) {
        int chunkCrc = checksumIn.readInt();

        if (compositeCrc == 0) {
          compositeCrc = chunkCrc;
        } else {
          compositeCrc = CrcUtil.composeWithMonomial(
              compositeCrc, chunkCrc, fullChunkMonomial, crcPolynomial);
        }

        if ((chunkNum + 1) % crcsPerStripe == 0) {
          // Commit a single striped crc and reset the current compositeCrc.
          CrcUtil.writeInt(stripedCrcs, stripedCrcsCursor, compositeCrc);
          stripedCrcsCursor += 4;
          compositeCrc = 0;
        }
      }

      // There may be a final partial chunk that is not full-sized. Unlike the
      // MD5 case, we still consider this a "partial chunk" even if
      // getRequestLength() == getVisibleLength(), since the CRC composition
      // depends on the byte size of that final chunk, even if it already has a
      // precomputed CRC stored in metadata. So there are two cases:
      //   1. Reading only part of a block via getRequestLength(); we get the
      //      crcPartialBlock() explicitly.
      //   2. Reading full visible length; the partial chunk already has a CRC
      //      stored in block metadata, so we just continue reading checksumIn.
      long partialChunkSize = checksumDataLength % getBytesPerCRC();
      if (partialChunkSize > 0) {
        int partialChunkCrc;
        if (isPartialBlk()) {
          byte[] partialChunkCrcBytes = crcPartialBlock();
          partialChunkCrc = CrcUtil.readInt(partialChunkCrcBytes, 0);
        } else {
          partialChunkCrc = checksumIn.readInt();
        }
        compositeCrc = CrcUtil.compose(
            compositeCrc, partialChunkCrc, partialChunkSize, crcPolynomial);
      }

      // TODO(dhuo): Find a cleaner way of accounting the various cases:
      // 1. Exactly line up with full block (no partial chunk, no uncommitted
      //    compositeCrc).
      // 2. No partial chunk, but partial stripe; compositeCrc still needs
      //    to be committed.
      // 3. Lined up on full chunks, but partial chunk == partial stripe
      if (stripedCrcsCursor < stripedCrcs.length) {
        CrcUtil.writeInt(stripedCrcs, stripedCrcsCursor, compositeCrc);
      }

      setOutBytes(stripedCrcs);
      // TODO(dhuo): Support striped crcs in the debug statement.
      LOG.debug("block={}, getBytesPerCRC={}, crcPerBlock={}, compositeCrc={}",
          getBlock(), getBytesPerCRC(), getCrcPerBlock(),
          String.format("0x%08x", compositeCrc));
    }
  }

  /**
   * Non-striped block group checksum computer for striped blocks.
   */
  static class BlockGroupNonStripedChecksumComputer
      extends AbstractBlockChecksumComputer {

    private final ExtendedBlock blockGroup;
    private final ErasureCodingPolicy ecPolicy;
    private final DatanodeInfo[] datanodes;
    private final Token<BlockTokenIdentifier>[] blockTokens;
    private final byte[] blockIndices;
    private final long requestedNumBytes;

    private final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

    // Keeps track of the positions within blockChecksumBuf where each data
    // block's checksum begins; for fixed-size block checksums this is easily
    // calculated as a multiple of the checksum size, but for striped block
    // CRCs, it's less error-prone to simply keep track of exact byte offsets
    // before each block checksum is populated into the buffer.
    private final int[] blockChecksumPositions;

    BlockGroupNonStripedChecksumComputer(DataNode datanode,
                                         StripedBlockInfo stripedBlockInfo,
                                         long requestedNumBytes,
                                         BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, blockChecksumOptions);
      this.blockGroup = stripedBlockInfo.getBlock();
      this.ecPolicy = stripedBlockInfo.getErasureCodingPolicy();
      this.datanodes = stripedBlockInfo.getDatanodes();
      this.blockTokens = stripedBlockInfo.getBlockTokens();
      this.blockIndices = stripedBlockInfo.getBlockIndices();
      this.requestedNumBytes = requestedNumBytes;
      this.blockChecksumPositions = new int[this.ecPolicy.getNumDataUnits()];
    }

    private static class LiveBlockInfo {
      private final DatanodeInfo dn;
      private final Token<BlockTokenIdentifier> token;

      LiveBlockInfo(DatanodeInfo dn, Token<BlockTokenIdentifier> token) {
        this.dn = dn;
        this.token = token;
      }

      DatanodeInfo getDn() {
        return dn;
      }

      Token<BlockTokenIdentifier> getToken() {
        return token;
      }
    }

    @Override
    void compute() throws IOException {
      assert datanodes.length == blockIndices.length;

      Map<Byte, LiveBlockInfo> liveDns = new HashMap<>(datanodes.length);
      int blkIndxLen = blockIndices.length;
      int numDataUnits = ecPolicy.getNumDataUnits();
      // Prepare live datanode list. Missing data blocks will be reconstructed
      // and recalculate checksum.
      for (int idx = 0; idx < blkIndxLen; idx++) {
        liveDns.put(blockIndices[idx],
            new LiveBlockInfo(datanodes[idx], blockTokens[idx]));
      }
      long checksumLen = 0;
      for (int idx = 0; idx < numDataUnits && idx < blkIndxLen; idx++) {
        // Before populating the blockChecksum at this index, record the byte
        // offset where it will begin.
        blockChecksumPositions[idx] = blockChecksumBuf.getLength();
        try {
          ExtendedBlock block = getInternalBlock(numDataUnits, idx);

          LiveBlockInfo liveBlkInfo = liveDns.get((byte) idx);
          if (liveBlkInfo == null) {
            // reconstruct block and calculate checksum for missing node
            recalculateChecksum(idx, block.getNumBytes());
          } else {
            try {
              checksumBlock(block, idx, liveBlkInfo.getToken(),
                  liveBlkInfo.getDn());
            } catch (IOException ioe) {
              LOG.warn("Exception while reading checksum", ioe);
              // reconstruct block and calculate checksum for the failed node
              recalculateChecksum(idx, block.getNumBytes());
            }
          }
          checksumLen += block.getNumBytes();
          if (checksumLen >= requestedNumBytes) {
            break; // done with the computation, simply return.
          }
        } catch (IOException e) {
          LOG.warn("Failed to get the checksum", e);
        }
      }

      BlockChecksumType type = getBlockChecksumOptions().getBlockChecksumType();
      switch (type) {
        case MD5CRC:
          // TODO(dhuo): Check whether digest(DataOutputBuffer.getData() is a
          // latent bug due to the data buffer being longer than the valid data
          // length.
          MD5Hash md5out = MD5Hash.digest(blockChecksumBuf.getData());
          setOutBytes(md5out.getDigest());
          break;
        case COMPOSITE_CRC:
          // TODO(dhuo): Maybe also use the monomial precompute.
          int compositeCrc = 0;
          int crcPolynomial =
              DataChecksum.getCrcPolynomialForType(getCrcType());
          // This should hold all the cell-granularity checksums of blk0
          // followed by all cell checksums of blk1, etc. We must unstripe the
          // cell checksums in order of logical file bytes. Also, note that the
          // length of this array may not equal the the number of actually valid
          // bytes in the buffer (blockChecksumBuf.getLength()).
          byte[] flatBlockChecksumData = blockChecksumBuf.getData();

          // Initialize byte-level cursors to where each block's checksum begins
          // inside the combined flattened buffer.
          int[] blockChecksumCursors = new int[numDataUnits];
          for (int idx = 0; idx < numDataUnits; ++idx) {
            blockChecksumCursors[idx] = blockChecksumPositions[idx];
          }

          // Reassemble cell-level CRCs in the right order.
          long numFullCells = checksumLen / ecPolicy.getCellSize();
          for (long cellIndex = 0; cellIndex < numFullCells; ++cellIndex) {
            int blockIndex = (int) (cellIndex % numDataUnits);
            int checksumCursor = blockChecksumCursors[blockIndex];
            int cellCrc = CrcUtil.readInt(
                flatBlockChecksumData, checksumCursor);
            blockChecksumCursors[blockIndex] += 4;
            if (compositeCrc == 0) {
              compositeCrc = cellCrc;
            } else {
              compositeCrc = CrcUtil.compose(
                  compositeCrc, cellCrc, ecPolicy.getCellSize(), crcPolynomial);
            }
          }
          if (checksumLen % ecPolicy.getCellSize() != 0) {
            // Final partial cell.
            int blockIndex = (int) (numFullCells % numDataUnits);
            int checksumCursor = blockChecksumCursors[blockIndex];
            int cellCrc = CrcUtil.readInt(
                flatBlockChecksumData, checksumCursor);
            blockChecksumCursors[blockIndex] += 4;
            compositeCrc = CrcUtil.compose(
                compositeCrc, cellCrc, checksumLen % ecPolicy.getCellSize(),
                crcPolynomial);
          }
          // TODO(dhuo): Maybe add a debug statement here.
          setOutBytes(CrcUtil.intToBytes(compositeCrc));
          break;
        default:
          throw new IOException(String.format(
              "Unrecognized BlockChecksumType: %s", type));
      }
    }

    private ExtendedBlock getInternalBlock(int numDataUnits, int idx) {
      // Sets requested number of bytes in blockGroup which is required to
      // construct the internal block for computing checksum.
      long actualNumBytes = blockGroup.getNumBytes();
      blockGroup.setNumBytes(requestedNumBytes);

      ExtendedBlock block = StripedBlockUtil.constructInternalBlock(blockGroup,
          ecPolicy.getCellSize(), numDataUnits, idx);

      // Set back actualNumBytes value in blockGroup.
      blockGroup.setNumBytes(actualNumBytes);
      return block;
    }

    private void checksumBlock(ExtendedBlock block, int blockIdx,
                               Token<BlockTokenIdentifier> blockToken,
                               DatanodeInfo targetDatanode) throws IOException {
      int timeout = 3000;
      try (IOStreamPair pair = getDatanode().connectToDN(targetDatanode,
          timeout, block, blockToken)) {

        LOG.debug("write to {}: {}, block={}",
            getDatanode(), Op.BLOCK_CHECKSUM, block);

        // get block checksum
        // A BlockGroupCheckum of type COMPOSITE_CRC uses underlying
        // BlockChecksums of type STRIPED_CRC.
        BlockChecksumOptions childOptions;
        BlockChecksumType groupChecksumType =
            getBlockChecksumOptions().getBlockChecksumType();
        switch (groupChecksumType) {
          case MD5CRC:
            childOptions = getBlockChecksumOptions();
            break;
          case COMPOSITE_CRC:
            childOptions = new BlockChecksumOptions(
                BlockChecksumType.STRIPED_CRC, ecPolicy.getCellSize());
            break;
          default:
            throw new IOException(
                "Unknown BlockChecksumType: " + groupChecksumType);
        }
        createSender(pair).blockChecksum(block, blockToken, childOptions);

        final DataTransferProtos.BlockOpResponseProto reply =
            DataTransferProtos.BlockOpResponseProto.parseFrom(
                PBHelperClient.vintPrefixed(pair.in));

        String logInfo = "for block " + block
            + " from datanode " + targetDatanode;
        DataTransferProtoUtil.checkBlockOpStatus(reply, logInfo);

        DataTransferProtos.OpBlockChecksumResponseProto checksumData =
            reply.getChecksumResponse();

        // read crc-type
        final DataChecksum.Type ct;
        if (checksumData.hasCrcType()) {
          ct = PBHelperClient.convert(checksumData.getCrcType());
        } else {
          LOG.debug("Retrieving checksum from an earlier-version DataNode: "
              + "inferring checksum by reading first byte");
          ct = DataChecksum.Type.DEFAULT;
        }

        setOrVerifyChecksumProperties(blockIdx, checksumData.getBytesPerCrc(),
            checksumData.getCrcPerBlock(), ct);

        switch (groupChecksumType) {
          case MD5CRC:
            //read md5
            final MD5Hash md5 =
                new MD5Hash(checksumData.getBlockChecksum().toByteArray());
            md5.write(blockChecksumBuf);
            LOG.debug("got reply from datanode:{}, md5={}",
                targetDatanode, md5);
            break;
          case COMPOSITE_CRC:
            BlockChecksumType returnedType = PBHelperClient.convert(
                checksumData.getBlockChecksumOptions().getBlockChecksumType());
            if (returnedType != BlockChecksumType.STRIPED_CRC) {
              throw new IOException(String.format(
                  "Unexpected blockChecksumType '%s', expecting STRIPED_CRC",
                  returnedType));
            }
            byte[] checksumBytes =
                checksumData.getBlockChecksum().toByteArray();
            blockChecksumBuf.write(checksumBytes, 0, checksumBytes.length);
            // TODO(dhuo): Add debug log for returned data.
            LOG.debug("got reply from datanode:{} for blockIdx:{}, length:{}",
                targetDatanode, blockIdx, checksumBytes.length);
            break;
          default:
            throw new IOException(
                "Unknown BlockChecksumType: " + groupChecksumType);
        }
      }
    }

    /**
     * Reconstruct this data block and recalculate checksum.
     *
     * @param errBlkIndex
     *          error index to be reconstructed and recalculate checksum.
     * @param blockLength
     *          number of bytes in the block to compute checksum.
     * @throws IOException
     */
    private void recalculateChecksum(int errBlkIndex, long blockLength)
        throws IOException {
      LOG.debug("Recalculate checksum for the missing/failed block index {}",
          errBlkIndex);
      byte[] errIndices = new byte[1];
      errIndices[0] = (byte) errBlkIndex;

      StripedReconstructionInfo stripedReconInfo =
          new StripedReconstructionInfo(
              blockGroup, ecPolicy, blockIndices, datanodes, errIndices);
      // TODO(dhuo): Plumb through to StripedBlockChecksumReconstructor
      final StripedBlockChecksumReconstructor checksumRecon =
          new StripedBlockChecksumMd5CrcReconstructor(
              getDatanode().getErasureCodingWorker(), stripedReconInfo,
              blockChecksumBuf, blockLength);
      checksumRecon.reconstruct();

      DataChecksum checksum = checksumRecon.getChecksum();
      long crcPerBlock = checksum.getChecksumSize() <= 0 ? 0
          : checksumRecon.getChecksumDataLen() / checksum.getChecksumSize();
      setOrVerifyChecksumProperties(errBlkIndex,
          checksum.getBytesPerChecksum(), crcPerBlock,
          checksum.getChecksumType());
      LOG.debug("Recalculated checksum for the block index:{}, checksum={}",
          errBlkIndex, checksumRecon.getDigestObject());
    }

    private void setOrVerifyChecksumProperties(int blockIdx, int bpc,
        final long cpb, DataChecksum.Type ct) throws IOException {
      //read byte-per-checksum
      if (blockIdx == 0) { //first block
        setBytesPerCRC(bpc);
      } else if (bpc != getBytesPerCRC()) {
        throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
            + " but bytesPerCRC=" + getBytesPerCRC());
      }

      //read crc-per-block
      if (blockIdx == 0) {
        setCrcPerBlock(cpb);
      }

      if (blockIdx == 0) { // first block
        setCrcType(ct);
      } else if (getCrcType() != DataChecksum.Type.MIXED &&
          getCrcType() != ct) {
        // TODO(dhuo): Throw if getBlockChecksumType() == COMPOSITE_CTC
        // if crc types are mixed in a file
        setCrcType(DataChecksum.Type.MIXED);
      }

      if (blockIdx == 0) {
        LOG.debug("set bytesPerCRC={}, crcPerBlock={}", getBytesPerCRC(),
            getCrcPerBlock());
      }
    }
  }
}
