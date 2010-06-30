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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * This class implements a simulated FSDataset.
 * 
 * Blocks that are created are recorded but their data (plus their CRCs) are
 *  discarded.
 * Fixed data is returned when blocks are read; a null CRC meta file is
 * created for such data.
 * 
 * This FSDataset does not remember any block information across its
 * restarts; it does however offer an operation to inject blocks
 *  (See the TestInectionForSImulatedStorage()
 * for a usage example of injection.
 * 
 * Note the synchronization is coarse grained - it is at each method. 
 */

public class SimulatedFSDataset  implements FSConstants, FSDatasetInterface, Configurable{
  
  public static final String CONFIG_PROPERTY_SIMULATED =
                                    "dfs.datanode.simulateddatastorage";
  public static final String CONFIG_PROPERTY_CAPACITY =
                            "dfs.datanode.simulateddatastorage.capacity";
  
  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte
  public static final byte DEFAULT_DATABYTE = 9; // 1 terabyte
  byte simulatedDataByte = DEFAULT_DATABYTE;
  Configuration conf = null;
  
  static byte[] nullCrcFileData;
  {
    DataChecksum checksum = DataChecksum.newDataChecksum( DataChecksum.
                              CHECKSUM_NULL, 16*1024 );
    byte[] nullCrcHeader = checksum.getHeader();
    nullCrcFileData =  new byte[2 + nullCrcHeader.length];
    nullCrcFileData[0] = (byte) ((FSDataset.METADATA_VERSION >>> 8) & 0xff);
    nullCrcFileData[1] = (byte) (FSDataset.METADATA_VERSION & 0xff);
    for (int i = 0; i < nullCrcHeader.length; i++) {
      nullCrcFileData[i+2] = nullCrcHeader[i];
    }
  }

  // information about a single block
  private class BInfo implements ReplicaInPipelineInterface {
    Block theBlock;
    private boolean finalized = false; // if not finalized => ongoing creation
    SimulatedOutputStream oStream = null;
    private long bytesAcked;
    private long bytesRcvd;
    BInfo(Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0) {
        theBlock.setNumBytes(0);
      }
      if (!storage.alloc(theBlock.getNumBytes())) { // expected length - actual length may
                                          // be more - we find out at finalize
        DataNode.LOG.warn("Lack of free storage on a block alloc");
        throw new IOException("Creating block, no free space available");
      }

      if (forWriting) {
        finalized = false;
        oStream = new SimulatedOutputStream();
      } else {
        finalized = true;
        oStream = null;
      }
    }

    synchronized public long getGenerationStamp() {
      return theBlock.getGenerationStamp();
    }

    synchronized public long getNumBytes() {
      if (!finalized) {
         return bytesRcvd;
      } else {
        return theBlock.getNumBytes();
      }
    }

    synchronized public void setNumBytes(long length) {
      if (!finalized) {
         bytesRcvd = length;
      } else {
        theBlock.setNumBytes(length);
      }
    }
    
    synchronized SimulatedInputStream getIStream() throws IOException {
      if (!finalized) {
        // throw new IOException("Trying to read an unfinalized block");
         return new SimulatedInputStream(oStream.getLength(), DEFAULT_DATABYTE);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), DEFAULT_DATABYTE);
      }
    }
    
    synchronized void finalizeBlock(long finalSize) throws IOException {
      if (finalized) {
        throw new IOException(
            "Finalizing a block that has already been finalized" + 
            theBlock.getBlockId());
      }
      if (oStream == null) {
        DataNode.LOG.error("Null oStream on unfinalized block - bug");
        throw new IOException("Unexpected error on finalize");
      }

      if (oStream.getLength() != finalSize) {
        DataNode.LOG.warn("Size passed to finalize (" + finalSize +
                    ")does not match what was written:" + oStream.getLength());
        throw new IOException(
          "Size passed to finalize does not match the amount of data written");
      }
      // We had allocated the expected length when block was created; 
      // adjust if necessary
      long extraLen = finalSize - theBlock.getNumBytes();
      if (extraLen > 0) {
        if (!storage.alloc(extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        storage.free(-extraLen);
      }
      theBlock.setNumBytes(finalSize);  

      finalized = true;
      oStream = null;
      return;
    }

    synchronized void unfinalizeBlock() throws IOException {
      if (!finalized) {
        throw new IOException("Unfinalized a block that's not finalized "
            + theBlock);
      }
      finalized = false;
      oStream = new SimulatedOutputStream();
      long blockLen = theBlock.getNumBytes();
      oStream.setLength(blockLen);
      bytesRcvd = blockLen;
      bytesAcked = blockLen;
    }

    SimulatedInputStream getMetaIStream() {
      return new SimulatedInputStream(nullCrcFileData);  
    }

    synchronized boolean isFinalized() {
      return finalized;
    }

    @Override
    synchronized public BlockWriteStreams createStreams(boolean isCreate, 
        int bytesPerChunk, int checksumSize) throws IOException {
      if (finalized) {
        throw new IOException("Trying to write to a finalized replica "
            + theBlock);
      } else {
        SimulatedOutputStream crcStream = new SimulatedOutputStream();
        return new BlockWriteStreams(oStream, crcStream);
      }
    }

    @Override
    synchronized public long getBlockId() {
      return theBlock.getBlockId();
    }

    @Override
    synchronized public long getVisibleLength() {
      return getBytesAcked();
    }

    @Override
    public ReplicaState getState() {
      return null;
    }

    @Override
    synchronized public long getBytesAcked() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return bytesAcked;
      }
    }

    @Override
    synchronized public void setBytesAcked(long bytesAcked) {
      if (!finalized) {
        this.bytesAcked = bytesAcked;
      }
    }

    @Override
    synchronized public long getBytesOnDisk() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return oStream.getLength();
      }
    }

    @Override
    public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
      oStream.setLength(dataLength);
    }

    @Override
    public ChunkChecksum getLastChecksumAndDataLen() {
      return new ChunkChecksum(oStream.getLength(), null);
    }
  }
  
  static private class SimulatedStorage {
    private long capacity;  // in bytes
    private long used;    // in bytes
    
    synchronized long getFree() {
      return capacity - used;
    }
    
    synchronized long getCapacity() {
      return capacity;
    }
    
    synchronized long getUsed() {
      return used;
    }
    
    synchronized boolean alloc(long amount) {
      if (getFree() >= amount) {
        used += amount;
        return true;
      } else {
        return false;    
      }
    }
    
    synchronized void free(long amount) {
      used -= amount;
    }
    
    SimulatedStorage(long cap) {
      capacity = cap;
      used = 0;   
    }
  }
  
  private HashMap<Block, BInfo> blockMap = null;
  private SimulatedStorage storage = null;
  private String storageId;
  
  public SimulatedFSDataset(Configuration conf) throws IOException {
    setConf(conf);
  }
  
  private SimulatedFSDataset() { // real construction when setConf called.. Uggg
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration iconf)  {
    conf = iconf;
    storageId = conf.get(DFSConfigKeys.DFS_DATANODE_STORAGEID_KEY, "unknownStorageId" +
                                        new Random().nextInt());
    registerMBean(storageId);
    storage = new SimulatedStorage(
        conf.getLong(CONFIG_PROPERTY_CAPACITY, DEFAULT_CAPACITY));
    //DataNode.LOG.info("Starting Simulated storage; Capacity = " + getCapacity() + 
    //    "Used = " + getDfsUsed() + "Free =" + getRemaining());

    blockMap = new HashMap<Block,BInfo>(); 
  }

  public synchronized void injectBlocks(Iterable<Block> injectBlocks)
                                            throws IOException {
    if (injectBlocks != null) {
      int numInjectedBlocks = 0;
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        numInjectedBlocks++;
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        if (isValidBlock(b)) {
          throw new IOException("Block already exists in  block list");
        }
      }
      HashMap<Block, BInfo> oldBlockMap = blockMap;
      blockMap = new HashMap<Block,BInfo>(
          numInjectedBlocks + oldBlockMap.size());
      blockMap.putAll(oldBlockMap);
      for (Block b: injectBlocks) {
          BInfo binfo = new BInfo(b, false);
          blockMap.put(binfo.theBlock, binfo);
      }
    }
  }

  public synchronized void finalizeBlock(Block b) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(b.getNumBytes());

  }

  public synchronized void unfinalizeBlock(Block b) throws IOException {
    if (isBeingWritten(b)) {
      blockMap.remove(b);
    }
  }

  public synchronized BlockListAsLongs getBlockReport() {
    Block[] blockTable = new Block[blockMap.size()];
    int count = 0;
    for (BInfo b : blockMap.values()) {
      if (b.isFinalized()) {
        blockTable[count++] = b.theBlock;
      }
    }
    if (count != blockTable.length) {
      blockTable = Arrays.copyOf(blockTable, count);
    }
    return new BlockListAsLongs(
        new ArrayList<Block>(Arrays.asList(blockTable)), null);
  }

  public long getCapacity() throws IOException {
    return storage.getCapacity();
  }

  public long getDfsUsed() throws IOException {
    return storage.getUsed();
  }

  public long getRemaining() throws IOException {
    return storage.getFree();
  }

  public synchronized long getLength(Block b) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    return binfo.getNumBytes();
  }

  @Override
  @Deprecated
  public Replica getReplica(long blockId) {
    return blockMap.get(new Block(blockId));
  }

  /** {@inheritDoc} */
  public Block getStoredBlock(long blkid) throws IOException {
    Block b = new Block(blkid);
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return null;
    }
    b.setGenerationStamp(binfo.getGenerationStamp());
    b.setNumBytes(binfo.getNumBytes());
    return b;
  }

  public synchronized void invalidate(Block[] invalidBlks) throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      BInfo binfo = blockMap.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      storage.free(binfo.getNumBytes());
      blockMap.remove(b);
    }
      if (error) {
          throw new IOException("Invalidate: Missing blocks.");
      }
  }

  public synchronized boolean isValidBlock(Block b) {
    // return (blockMap.containsKey(b));
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return false;
    }
    return binfo.isFinalized();
  }

  /* check if a block is created but not finalized */
  private synchronized boolean isBeingWritten(Block b) {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return false;
    }
    return !binfo.isFinalized();  
  }
  
  public String toString() {
    return getStorageInfo();
  }

  @Override
  public synchronized ReplicaInPipelineInterface append(Block b,
      long newGS, long expectedBlockLen) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null || !binfo.isFinalized()) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    binfo.unfinalizeBlock();
    return binfo;
  }

  @Override
  public synchronized ReplicaInPipelineInterface recoverAppend(Block b,
      long newGS, long expectedBlockLen) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      binfo.unfinalizeBlock();
    }
    blockMap.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    blockMap.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override
  public void recoverClose(Block b, long newGS,
      long expectedBlockLen) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (!binfo.isFinalized()) {
      binfo.finalizeBlock(binfo.getNumBytes());
    }
    blockMap.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    blockMap.put(binfo.theBlock, binfo);
  }
  
  @Override
  public synchronized ReplicaInPipelineInterface recoverRbw(Block b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException {
    BInfo binfo = blockMap.get(b);
    if ( binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " does not exist, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      throw new ReplicaAlreadyExistsException("Block " + b
          + " is valid, and cannot be written to.");
    }
    blockMap.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    blockMap.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override
  public synchronized ReplicaInPipelineInterface createRbw(Block b) 
  throws IOException {
    return createTemporary(b);
  }

  @Override
  public synchronized ReplicaInPipelineInterface createTemporary(Block b)
      throws IOException {
    if (isValidBlock(b)) {
          throw new ReplicaAlreadyExistsException("Block " + b + 
              " is valid, and cannot be written to.");
      }
    if (isBeingWritten(b)) {
        throw new ReplicaAlreadyExistsException("Block " + b + 
            " is being written, and cannot be written to.");
    }
    BInfo binfo = new BInfo(b, true);
    blockMap.put(binfo.theBlock, binfo);
    return binfo;
  }

  public synchronized InputStream getBlockInputStream(Block b)
                                            throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    
    //DataNode.LOG.info("Opening block(" + b.blkid + ") of length " + b.len);
    return binfo.getIStream();
  }
  
  public synchronized InputStream getBlockInputStream(Block b, long seekOffset)
                              throws IOException {
    InputStream result = getBlockInputStream(b);
    result.skip(seekOffset);
    return result;
  }

  /** Not supported */
  public BlockInputStreams getTmpInputStreams(Block b, long blkoff, long ckoff
      ) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Returns metaData of block b as an input stream
   * @param b - the block for which the metadata is desired
   * @return metaData of block b as an input stream
   * @throws IOException - block does not exist or problems accessing
   *  the meta file
   */
  private synchronized InputStream getMetaDataInStream(Block b)
                                              throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b + 
          " is being written, its meta cannot be read");
    }
    return binfo.getMetaIStream();
  }

  public synchronized long getMetaDataLength(Block b) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its metalength cannot be read");
    }
    return binfo.getMetaIStream().getLength();
  }
  
  public MetaDataInputStream getMetaDataInputStream(Block b)
  throws IOException {

       return new MetaDataInputStream(getMetaDataInStream(b),
                                                getMetaDataLength(b));
  }

  public synchronized boolean metaFileExists(Block b) throws IOException {
    if (!isValidBlock(b)) {
          throw new IOException("Block " + b +
              " is valid, and cannot be written to.");
      }
    return true; // crc exists for all valid blocks
  }

  public void checkDataDir() throws DiskErrorException {
    // nothing to check for simulated data set
  }

  @Override
  public synchronized void adjustCrcChannelPosition(Block b,
                                              BlockWriteStreams stream, 
                                              int checksumSize)
                                              throws IOException {
  }

  /** 
   * Simulated input and output streams
   *
   */
  static private class SimulatedInputStream extends java.io.InputStream {
    

    byte theRepeatedData = 7;
    long length; // bytes
    int currentPos = 0;
    byte[] data = null;
    
    /**
     * An input stream of size l with repeated bytes
     * @param l
     * @param iRepeatedData
     */
    SimulatedInputStream(long l, byte iRepeatedData) {
      length = l;
      theRepeatedData = iRepeatedData;
    }
    
    /**
     * An input stream of of the supplied data
     * 
     * @param iData
     */
    SimulatedInputStream(byte[] iData) {
      data = iData;
      length = data.length;
      
    }
    
    /**
     * 
     * @return the lenght of the input stream
     */
    long getLength() {
      return length;
    }

    @Override
    public int read() throws IOException {
      if (currentPos >= length)
        return -1;
      if (data !=null) {
        return data[currentPos++];
      } else {
        currentPos++;
        return theRepeatedData;
      }
    }
    
    @Override
    public int read(byte[] b) throws IOException { 

      if (b == null) {
        throw new NullPointerException();
      }
      if (b.length == 0) {
        return 0;
      }
      if (currentPos >= length) { // EOF
        return -1;
      }
      int bytesRead = (int) Math.min(b.length, length-currentPos);
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i : b) {  
          b[i] = theRepeatedData;
        }
      }
      currentPos += bytesRead;
      return bytesRead;
    }
  }
  
  /**
   * This class implements an output stream that merely throws its data away, but records its
   * length.
   *
   */
  static private class SimulatedOutputStream extends OutputStream {
    long length = 0;
    
    /**
     * constructor for Simulated Output Steram
     */
    SimulatedOutputStream() {
    }
    
    /**
     * 
     * @return the length of the data created so far.
     */
    long getLength() {
      return length;
    }

    /**
     */
    void setLength(long length) {
      this.length = length;
    }
    
    @Override
    public void write(int arg0) throws IOException {
      length++;
    }
    
    @Override
    public void write(byte[] b) throws IOException {
      length += b.length;
    }
    
    @Override
    public void write(byte[] b,
              int off,
              int len) throws IOException  {
      length += len;
    }
  }
  
  private ObjectName mbeanName;


  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   *  We use storage id for MBean name since a minicluster within a single
   * Java VM may have multiple Simulated Datanodes.
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;

    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean("DataNode",
          "FSDatasetState-" + storageId, bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  public String getStorageInfo() {
    return "Simulated FSDataset-" + storageId;
  }
  
  public boolean hasEnoughResource() {
    return true;
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException {
    Block b = rBlock.getBlock();
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }

    return new ReplicaRecoveryInfo(binfo.getBlockId(), binfo.getBytesOnDisk(), 
        binfo.getGenerationStamp(), 
        binfo.isFinalized()?ReplicaState.FINALIZED : ReplicaState.RBW);
  }

  @Override // FSDatasetInterface
  public FinalizedReplica updateReplicaUnderRecovery(Block oldBlock,
                                        long recoveryId,
                                        long newlength) throws IOException {
    return new FinalizedReplica(
        oldBlock.getBlockId(), newlength, recoveryId, null, null);
  }

  @Override
  public long getReplicaVisibleLength(Block block) throws IOException {
    return block.getNumBytes();
  }
}
