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

import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Utility classes to compute file checksum for both replicated and striped
 * files.
 */
final class FileChecksumHelper {
  static final Logger LOG =
      LoggerFactory.getLogger(FileChecksumHelper.class);

  private FileChecksumHelper() {}

  /**
   * A common abstract class to compute file checksum.
   */
  static abstract class FileChecksumComputer {
    private final String src;
    private final long length;
    private final DFSClient client;
    private final ClientProtocol namenode;
    private final DataOutputBuffer md5out = new DataOutputBuffer();

    private MD5MD5CRC32FileChecksum fileChecksum;
    private LocatedBlocks blockLocations;

    private int timeout;
    private List<LocatedBlock> locatedBlocks;
    private long remaining = 0L;

    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
    private long crcPerBlock = 0;
    private boolean isRefetchBlocks = false;
    private int lastRetriedIndex = -1;

    /**
     * Constructor that accepts all the input parameters for the computing.
     */
    FileChecksumComputer(String src, long length,
                         LocatedBlocks blockLocations,
                         ClientProtocol namenode,
                         DFSClient client) throws IOException {
      this.src = src;
      this.length = length;
      this.blockLocations = blockLocations;
      this.namenode = namenode;
      this.client = client;

      this.remaining = length;

      if (blockLocations != null) {
        if (src.contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR)) {
          this.remaining = Math.min(length, blockLocations.getFileLength());
        }
        this.locatedBlocks = blockLocations.getLocatedBlocks();
      }
    }

    String getSrc() {
      return src;
    }

    long getLength() {
      return length;
    }

    DFSClient getClient() {
      return client;
    }

    ClientProtocol getNamenode() {
      return namenode;
    }

    DataOutputBuffer getMd5out() {
      return md5out;
    }

    MD5MD5CRC32FileChecksum getFileChecksum() {
      return fileChecksum;
    }

    LocatedBlocks getBlockLocations() {
      return blockLocations;
    }

    void refetchBlocks() throws IOException {
      this.blockLocations = getClient().getBlockLocations(getSrc(),
          getLength());
      this.locatedBlocks = getBlockLocations().getLocatedBlocks();
      this.isRefetchBlocks = false;
    }

    int getTimeout() {
      return timeout;
    }

    void setTimeout(int timeout) {
      this.timeout = timeout;
    }

    List<LocatedBlock> getLocatedBlocks() {
      return locatedBlocks;
    }

    long getRemaining() {
      return remaining;
    }

    void setRemaining(long remaining) {
      this.remaining = remaining;
    }

    int getBytesPerCRC() {
      return bytesPerCRC;
    }

    void setBytesPerCRC(int bytesPerCRC) {
      this.bytesPerCRC = bytesPerCRC;
    }

    DataChecksum.Type getCrcType() {
      return crcType;
    }

    void setCrcType(DataChecksum.Type crcType) {
      this.crcType = crcType;
    }

    long getCrcPerBlock() {
      return crcPerBlock;
    }

    void setCrcPerBlock(long crcPerBlock) {
      this.crcPerBlock = crcPerBlock;
    }

    boolean isRefetchBlocks() {
      return isRefetchBlocks;
    }

    void setRefetchBlocks(boolean refetchBlocks) {
      this.isRefetchBlocks = refetchBlocks;
    }

    int getLastRetriedIndex() {
      return lastRetriedIndex;
    }

    void setLastRetriedIndex(int lastRetriedIndex) {
      this.lastRetriedIndex = lastRetriedIndex;
    }

    /**
     * Perform the file checksum computing. The intermediate results are stored
     * in the object and will be used later.
     * @throws IOException
     */
    void compute() throws IOException {
      /**
       * request length is 0 or the file is empty, return one with the
       * magic entry that matches what previous hdfs versions return.
       */
      if (locatedBlocks == null || locatedBlocks.isEmpty()) {
        // Explicitly specified here in case the default DataOutputBuffer
        // buffer length value is changed in future. This matters because the
        // fixed value 32 has to be used to repeat the magic value for previous
        // HDFS version.
        final int lenOfZeroBytes = 32;
        byte[] emptyBlockMd5 = new byte[lenOfZeroBytes];
        MD5Hash fileMD5 = MD5Hash.digest(emptyBlockMd5);
        fileChecksum =  new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
      } else {
        checksumBlocks();
        fileChecksum = makeFinalResult();
      }
    }

    /**
     * Compute and aggregate block checksums block by block.
     * @throws IOException
     */
    abstract void checksumBlocks() throws IOException;

    /**
     * Make final file checksum result given the computing process done.
     */
    MD5MD5CRC32FileChecksum makeFinalResult() {
      //compute file MD5
      final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData());
      switch (crcType) {
      case CRC32:
        return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      case CRC32C:
        return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      default:
        // we will get here when crcType is "NULL".
        return null;
      }
    }

    /**
     * Create and return a sender given an IO stream pair.
     */
    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }

    /**
     * Close an IO stream pair.
     */
    void close(IOStreamPair pair) {
      if (pair != null) {
        IOUtils.closeStream(pair.in);
        IOUtils.closeStream(pair.out);
      }
    }
  }

  /**
   * Replicated file checksum computer.
   */
  static class ReplicatedFileChecksumComputer extends FileChecksumComputer {
    private int blockIdx;

    ReplicatedFileChecksumComputer(String src, long length,
                                   LocatedBlocks blockLocations,
                                   ClientProtocol namenode,
                                   DFSClient client) throws IOException {
      super(src, length, blockLocations, namenode, client);
    }

    @Override
    void checksumBlocks() throws IOException {
      // get block checksum for each block
      for (blockIdx = 0;
           blockIdx < getLocatedBlocks().size() && getRemaining() >= 0;
           blockIdx++) {
        if (isRefetchBlocks()) {  // refetch to get fresh tokens
          refetchBlocks();
        }

        LocatedBlock locatedBlock = getLocatedBlocks().get(blockIdx);

        if (!checksumBlock(locatedBlock)) {
          throw new IOException("Fail to get block MD5 for " + locatedBlock);
        }
      }
    }

    /**
     * Return true when sounds good to continue or retry, false when severe
     * condition or totally failed.
     */
    private boolean checksumBlock(LocatedBlock locatedBlock) {
      ExtendedBlock block = locatedBlock.getBlock();
      if (getRemaining() < block.getNumBytes()) {
        block.setNumBytes(getRemaining());
      }
      setRemaining(getRemaining() - block.getNumBytes());

      DatanodeInfo[] datanodes = locatedBlock.getLocations();

      int tmpTimeout = 3000 * datanodes.length +
          getClient().getConf().getSocketTimeout();
      setTimeout(tmpTimeout);

      //try each datanode location of the block
      boolean done = false;
      for (int j = 0; !done && j < datanodes.length; j++) {
        try {
          tryDatanode(locatedBlock, datanodes[j]);
          done = true;
        } catch (InvalidBlockTokenException ibte) {
          if (blockIdx > getLastRetriedIndex()) {
            LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
                    + "for file {} for block {} from datanode {}. Will retry "
                    + "the block once.",
                getSrc(), block, datanodes[j]);
            setLastRetriedIndex(blockIdx);
            done = true; // actually it's not done; but we'll retry
            blockIdx--; // repeat at blockIdx-th block
            setRefetchBlocks(true);
          }
        } catch (InvalidEncryptionKeyException iee) {
          if (blockIdx > getLastRetriedIndex()) {
            LOG.debug("Got invalid encryption key error in response to "
                    + "OP_BLOCK_CHECKSUM for file {} for block {} from "
                    + "datanode {}. Will retry " + "the block once.",
                  getSrc(), block, datanodes[j]);
            setLastRetriedIndex(blockIdx);
            done = true; // actually it's not done; but we'll retry
            blockIdx--; // repeat at i-th block
            getClient().clearDataEncryptionKey();
          }
        } catch (IOException ie) {
          LOG.warn("src={}" + ", datanodes[{}]={}",
              getSrc(), j, datanodes[j], ie);
        }
      }

      return done;
    }

    /**
     * Try one replica or datanode to compute the block checksum given a block.
     */
    private void tryDatanode(LocatedBlock locatedBlock,
                             DatanodeInfo datanode) throws IOException {

      ExtendedBlock block = locatedBlock.getBlock();

      try (IOStreamPair pair = getClient().connectToDN(datanode, getTimeout(),
          locatedBlock.getBlockToken())) {

        LOG.debug("write to {}: {}, block={}", datanode,
            Op.BLOCK_CHECKSUM, block);

        // get block MD5
        createSender(pair).blockChecksum(block,
            locatedBlock.getBlockToken());

        final BlockOpResponseProto reply = BlockOpResponseProto.parseFrom(
            PBHelperClient.vintPrefixed(pair.in));

        String logInfo = "for block " + block + " from datanode " +
            datanode;
        DataTransferProtoUtil.checkBlockOpStatus(reply, logInfo);

        OpBlockChecksumResponseProto checksumData =
            reply.getChecksumResponse();

        //read byte-per-checksum
        final int bpc = checksumData.getBytesPerCrc();
        if (blockIdx == 0) { //first block
          setBytesPerCRC(bpc);
        } else if (bpc != getBytesPerCRC()) {
          throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
              + " but bytesPerCRC=" + getBytesPerCRC());
        }

        //read crc-per-block
        final long cpb = checksumData.getCrcPerBlock();
        if (getLocatedBlocks().size() > 1 && blockIdx == 0) {
          setCrcPerBlock(cpb);
        }

        //read md5
        final MD5Hash md5 = new MD5Hash(checksumData.getMd5().toByteArray());
        md5.write(getMd5out());

        // read crc-type
        final DataChecksum.Type ct;
        if (checksumData.hasCrcType()) {
          ct = PBHelperClient.convert(checksumData.getCrcType());
        } else {
          LOG.debug("Retrieving checksum from an earlier-version DataNode: " +
              "inferring checksum by reading first byte");
          ct = getClient().inferChecksumTypeByReading(locatedBlock, datanode);
        }

        if (blockIdx == 0) { // first block
          setCrcType(ct);
        } else if (getCrcType() != DataChecksum.Type.MIXED
            && getCrcType() != ct) {
          // if crc types are mixed in a file
          setCrcType(DataChecksum.Type.MIXED);
        }

        if (LOG.isDebugEnabled()) {
          if (blockIdx == 0) {
            LOG.debug("set bytesPerCRC=" + getBytesPerCRC()
                + ", crcPerBlock=" + getCrcPerBlock());
          }
          LOG.debug("got reply from " + datanode + ": md5=" + md5);
        }
      }
    }
  }

  /**
   * Non-striped checksum computing for striped files.
   */
  static class StripedFileNonStripedChecksumComputer
      extends FileChecksumComputer {
    private final ErasureCodingPolicy ecPolicy;
    private int bgIdx;

    StripedFileNonStripedChecksumComputer(String src, long length,
                                          LocatedBlocks blockLocations,
                                          ClientProtocol namenode,
                                          DFSClient client,
                                          ErasureCodingPolicy ecPolicy)
        throws IOException {
      super(src, length, blockLocations, namenode, client);

      this.ecPolicy = ecPolicy;
    }

    @Override
    void checksumBlocks() throws IOException {
      int tmpTimeout = 3000 * 1 + getClient().getConf().getSocketTimeout();
      setTimeout(tmpTimeout);

      for (bgIdx = 0;
           bgIdx < getLocatedBlocks().size() && getRemaining() >= 0; bgIdx++) {
        if (isRefetchBlocks()) {  // refetch to get fresh tokens
          refetchBlocks();
        }

        LocatedBlock locatedBlock = getLocatedBlocks().get(bgIdx);
        LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;

        if (!checksumBlockGroup(blockGroup)) {
          throw new IOException("Fail to get block MD5 for " + locatedBlock);
        }
      }
    }


    private boolean checksumBlockGroup(
        LocatedStripedBlock blockGroup) throws IOException {
      ExtendedBlock block = blockGroup.getBlock();
      long requestedNumBytes = block.getNumBytes();
      if (getRemaining() < block.getNumBytes()) {
        requestedNumBytes = getRemaining();
      }
      setRemaining(getRemaining() - requestedNumBytes);

      StripedBlockInfo stripedBlockInfo = new StripedBlockInfo(block,
          blockGroup.getLocations(), blockGroup.getBlockTokens(),
          blockGroup.getBlockIndices(), ecPolicy);
      DatanodeInfo[] datanodes = blockGroup.getLocations();

      //try each datanode in the block group.
      boolean done = false;
      for (int j = 0; !done && j < datanodes.length; j++) {
        try {
          tryDatanode(blockGroup, stripedBlockInfo, datanodes[j],
              requestedNumBytes);
          done = true;
        } catch (InvalidBlockTokenException ibte) {
          if (bgIdx > getLastRetriedIndex()) {
            LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
                    + "for file {} for block {} from datanode {}. Will retry "
                    + "the block once.",
                getSrc(), block, datanodes[j]);
            setLastRetriedIndex(bgIdx);
            done = true; // actually it's not done; but we'll retry
            bgIdx--; // repeat at bgIdx-th block
            setRefetchBlocks(true);
          }
        } catch (IOException ie) {
          LOG.warn("src={}" + ", datanodes[{}]={}",
              getSrc(), j, datanodes[j], ie);
        }
      }

      return done;
    }

    /**
     * Return true when sounds good to continue or retry, false when severe
     * condition or totally failed.
     */
    private void tryDatanode(LocatedStripedBlock blockGroup,
                             StripedBlockInfo stripedBlockInfo,
                             DatanodeInfo datanode,
                             long requestedNumBytes) throws IOException {

      try (IOStreamPair pair = getClient().connectToDN(datanode,
          getTimeout(), blockGroup.getBlockToken())) {

        LOG.debug("write to {}: {}, blockGroup={}",
            datanode, Op.BLOCK_GROUP_CHECKSUM, blockGroup);

        // get block MD5
        createSender(pair).blockGroupChecksum(stripedBlockInfo,
            blockGroup.getBlockToken(), requestedNumBytes);

        BlockOpResponseProto reply = BlockOpResponseProto.parseFrom(
            PBHelperClient.vintPrefixed(pair.in));

        String logInfo = "for blockGroup " + blockGroup +
            " from datanode " + datanode;
        DataTransferProtoUtil.checkBlockOpStatus(reply, logInfo);

        OpBlockChecksumResponseProto checksumData = reply.getChecksumResponse();

        //read byte-per-checksum
        final int bpc = checksumData.getBytesPerCrc();
        if (bgIdx == 0) { //first block
          setBytesPerCRC(bpc);
        } else {
          if (bpc != getBytesPerCRC()) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + getBytesPerCRC());
          }
        }

        //read crc-per-block
        final long cpb = checksumData.getCrcPerBlock();
        if (getLocatedBlocks().size() > 1 && bgIdx == 0) { // first block
          setCrcPerBlock(cpb);
        }

        //read md5
        final MD5Hash md5 = new MD5Hash(
            checksumData.getMd5().toByteArray());
        md5.write(getMd5out());

        // read crc-type
        final DataChecksum.Type ct;
        if (checksumData.hasCrcType()) {
          ct = PBHelperClient.convert(checksumData.getCrcType());
        } else {
          LOG.debug("Retrieving checksum from an earlier-version DataNode: " +
              "inferring checksum by reading first byte");
          ct = getClient().inferChecksumTypeByReading(blockGroup, datanode);
        }

        if (bgIdx == 0) {
          setCrcType(ct);
        } else if (getCrcType() != DataChecksum.Type.MIXED &&
            getCrcType() != ct) {
          // if crc types are mixed in a file
          setCrcType(DataChecksum.Type.MIXED);
        }

        if (LOG.isDebugEnabled()) {
          if (bgIdx == 0) {
            LOG.debug("set bytesPerCRC=" + getBytesPerCRC()
                + ", crcPerBlock=" + getCrcPerBlock());
          }
          LOG.debug("got reply from " + datanode + ": md5=" + md5);
        }
      }
    }
  }
}
