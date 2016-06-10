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
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedReconstructionInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.token.Token;
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
   * The abstract base block checksum computer.
   */
  static abstract class AbstractBlockChecksumComputer {
    private final DataNode datanode;

    private byte[] outBytes;
    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = null;
    private long crcPerBlock = -1;
    private int checksumSize = -1;

    AbstractBlockChecksumComputer(DataNode datanode) throws IOException {
      this.datanode = datanode;
    }

    abstract void compute() throws IOException;

    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }

    DataNode getDatanode() {
      return datanode;
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
   * The abstract base block checksum computer.
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
                          ExtendedBlock block) throws IOException {
      super(datanode);
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
                                    ExtendedBlock block) throws IOException {
      super(datanode, block);
    }

    @Override
    void compute() throws IOException {
      try {
        readHeader();

        MD5Hash md5out;
        if (isPartialBlk() && getCrcPerBlock() > 0) {
          md5out = checksumPartialBlock();
        } else {
          md5out = checksumWholeBlock();
        }
        setOutBytes(md5out.getDigest());

        if (LOG.isDebugEnabled()) {
          LOG.debug("block=" + getBlock() + ", bytesPerCRC=" + getBytesPerCRC()
              + ", crcPerBlock=" + getCrcPerBlock() + ", md5out=" + md5out);
        }
      } finally {
        IOUtils.closeStream(getChecksumIn());
        IOUtils.closeStream(getMetadataIn());
      }
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

    private final DataOutputBuffer md5writer = new DataOutputBuffer();

    BlockGroupNonStripedChecksumComputer(DataNode datanode,
                                         StripedBlockInfo stripedBlockInfo)
        throws IOException {
      super(datanode);
      this.blockGroup = stripedBlockInfo.getBlock();
      this.ecPolicy = stripedBlockInfo.getErasureCodingPolicy();
      this.datanodes = stripedBlockInfo.getDatanodes();
      this.blockTokens = stripedBlockInfo.getBlockTokens();
      this.blockIndices = stripedBlockInfo.getBlockIndices();
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
      for (int idx = 0; idx < numDataUnits && idx < blkIndxLen; idx++) {
        try {
          LiveBlockInfo liveBlkInfo = liveDns.get((byte) idx);
          if (liveBlkInfo == null) {
            // reconstruct block and calculate checksum for missing node
            recalculateChecksum(idx);
          } else {
            try {
              ExtendedBlock block = StripedBlockUtil.constructInternalBlock(
                  blockGroup, ecPolicy.getCellSize(), numDataUnits, idx);
              checksumBlock(block, idx, liveBlkInfo.getToken(),
                  liveBlkInfo.getDn());
            } catch (IOException ioe) {
              LOG.warn("Exception while reading checksum", ioe);
              // reconstruct block and calculate checksum for the failed node
              recalculateChecksum(idx);
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to get the checksum", e);
        }
      }

      MD5Hash md5out = MD5Hash.digest(md5writer.getData());
      setOutBytes(md5out.getDigest());
    }

    private void checksumBlock(ExtendedBlock block, int blockIdx,
                               Token<BlockTokenIdentifier> blockToken,
                               DatanodeInfo targetDatanode) throws IOException {
      int timeout = 3000;
      try (IOStreamPair pair = getDatanode().connectToDN(targetDatanode,
          timeout, block, blockToken)) {

        LOG.debug("write to {}: {}, block={}",
            getDatanode(), Op.BLOCK_CHECKSUM, block);

        // get block MD5
        createSender(pair).blockChecksum(block, blockToken);

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
        //read md5
        final MD5Hash md5 = new MD5Hash(checksumData.getMd5().toByteArray());
        md5.write(md5writer);
        if (LOG.isDebugEnabled()) {
          LOG.debug("got reply from " + targetDatanode + ": md5=" + md5);
        }
      }
    }

    /**
     * Reconstruct this data block and recalculate checksum.
     *
     * @param errBlkIndex
     *          error index to be reconstrcuted and recalculate checksum.
     * @throws IOException
     */
    private void recalculateChecksum(int errBlkIndex) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Recalculate checksum for the missing/failed block index "
            + errBlkIndex);
      }
      byte[] errIndices = new byte[1];
      errIndices[0] = (byte) errBlkIndex;
      StripedReconstructionInfo stripedReconInfo =
          new StripedReconstructionInfo(
          blockGroup, ecPolicy, blockIndices, datanodes, errIndices);
      final StripedBlockChecksumReconstructor checksumRecon =
          new StripedBlockChecksumReconstructor(
          getDatanode().getErasureCodingWorker(), stripedReconInfo,
          md5writer);
      checksumRecon.reconstruct();

      DataChecksum checksum = checksumRecon.getChecksum();
      long crcPerBlock = checksum.getChecksumSize() <= 0 ? 0
          : checksumRecon.getChecksumDataLen() / checksum.getChecksumSize();
      setOrVerifyChecksumProperties(errBlkIndex, checksum.getBytesPerChecksum(),
          crcPerBlock, checksum.getChecksumType());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Recalculated checksum for the block index " + errBlkIndex
            + ": md5=" + checksumRecon.getMD5());
      }
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
        // if crc types are mixed in a file
        setCrcType(DataChecksum.Type.MIXED);
      }

      if (LOG.isDebugEnabled()) {
        if (blockIdx == 0) {
          LOG.debug("set bytesPerCRC=" + getBytesPerCRC()
              + ", crcPerBlock=" + getCrcPerBlock());
        }
      }
    }
  }
}