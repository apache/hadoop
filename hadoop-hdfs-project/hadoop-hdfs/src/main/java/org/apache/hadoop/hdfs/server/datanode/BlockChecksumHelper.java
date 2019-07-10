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
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * Utilities for Block checksum computing, for both replicated and striped
 * blocks.
 */
final class BlockChecksumHelper {

  static final Logger LOG = LoggerFactory.getLogger(BlockChecksumHelper.class);

  private BlockChecksumHelper() {}

  /**
   * The abstract base block checksum computer.
   */
  static abstract class BlockChecksumComputer {
    private final DataNode datanode;
    private final BlockChecksumOptions blockChecksumOptions;
    private final ExtendedBlock block;
    // client side now can specify a range of the block for checksum
    private final long requestLength;
    private final LengthInputStream metadataIn;
    private final DataInputStream checksumIn;
    private final long visibleLength;
    private final boolean partialBlk;

    private byte[] outBytes;
    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = null;
    private long crcPerBlock = -1;
    private int checksumSize = -1;
    private BlockMetadataHeader header;
    private DataChecksum checksum;

    BlockChecksumComputer(DataNode datanode,
                          ExtendedBlock block,
                          BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      this.datanode = datanode;
      this.blockChecksumOptions = blockChecksumOptions;
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

    protected DataNode getDatanode() {
      return datanode;
    }

    protected ExtendedBlock getBlock() {
      return block;
    }

    protected long getRequestLength() {
      return requestLength;
    }

    protected LengthInputStream getMetadataIn() {
      return metadataIn;
    }

    protected DataInputStream getChecksumIn() {
      return checksumIn;
    }

    protected long getVisibleLength() {
      return visibleLength;
    }

    protected boolean isPartialBlk() {
      return partialBlk;
    }

    BlockChecksumOptions getBlockChecksumOptions() {
      return blockChecksumOptions;
    }

    protected void setOutBytes(byte[] bytes) {
      this.outBytes = bytes;
    }

    protected byte[] getOutBytes() {
      return outBytes;
    }

    protected int getBytesPerCRC() {
      return bytesPerCRC;
    }

    protected DataChecksum.Type getCrcType() {
      return crcType;
    }

    protected long getCrcPerBlock() {
      return crcPerBlock;
    }

    protected int getChecksumSize() {
      return checksumSize;
    }

    protected BlockMetadataHeader getHeader() {
      return header;
    }

    protected DataChecksum getChecksum() {
      return checksum;
    }

    /**
     * Perform the block checksum computing.
     * @throws IOException
     */
    abstract void compute() throws IOException;

    /**
     * Read block metadata header.
     * @throws IOException
     */
    protected void readHeader() throws IOException {
      //read metadata file
      header = BlockMetadataHeader.readHeader(checksumIn);
      checksum = header.getChecksum();
      checksumSize = checksum.getChecksumSize();
      bytesPerCRC = checksum.getBytesPerChecksum();
      crcPerBlock = checksumSize <= 0 ? 0 :
          (metadataIn.getLength() -
              BlockMetadataHeader.getHeaderSize()) / checksumSize;
      crcType = checksum.getChecksumType();
    }

    /**
     * Calculate partial block checksum.
     * @return
     * @throws IOException
     */
    protected byte[] crcPartialBlock() throws IOException {
      int partialLength = (int) (requestLength % bytesPerCRC);
      if (partialLength > 0) {
        byte[] buf = new byte[partialLength];
        final InputStream blockIn = datanode.data.getBlockInputStream(block,
            requestLength - partialLength);
        try {
          // Get the CRC of the partialLength.
          IOUtils.readFully(blockIn, buf, 0, partialLength);
        } finally {
          IOUtils.closeStream(blockIn);
        }
        checksum.update(buf, 0, partialLength);
        byte[] partialCrc = new byte[checksumSize];
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
      byte[] buffer = new byte[4*1024];
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

      CrcComposer crcComposer = CrcComposer.newStripedCrcComposer(
          getCrcType(), getBytesPerCRC(), stripeLength);
      DataInputStream checksumIn = getChecksumIn();

      // Whether getting the checksum for the entire block (which itself may
      // not be a full block size and may have a final chunk smaller than
      // getBytesPerCRC()), we begin with a number of full chunks, all of size
      // getBytesPerCRC().
      long numFullChunks = checksumDataLength / getBytesPerCRC();
      crcComposer.update(checksumIn, numFullChunks, getBytesPerCRC());

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
        if (isPartialBlk()) {
          byte[] partialChunkCrcBytes = crcPartialBlock();
          crcComposer.update(
              partialChunkCrcBytes, 0, partialChunkCrcBytes.length,
              partialChunkSize);
        } else {
          int partialChunkCrc = checksumIn.readInt();
          crcComposer.update(partialChunkCrc, partialChunkSize);
        }
      }

      byte[] composedCrcs = crcComposer.digest();
      setOutBytes(composedCrcs);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "block={}, getBytesPerCRC={}, crcPerBlock={}, compositeCrc={}",
            getBlock(), getBytesPerCRC(), getCrcPerBlock(),
            CrcUtil.toMultiCrcString(composedCrcs));
      }
    }
  }
}
