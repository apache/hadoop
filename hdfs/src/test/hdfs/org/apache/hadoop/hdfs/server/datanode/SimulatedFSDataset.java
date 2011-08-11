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
import java.util.Map;
import java.util.Random;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.BlockPoolSlice;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolumeSet;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.metrics2.util.MBeans;
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

public class SimulatedFSDataset  implements FSDatasetInterface, Configurable{
  
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
    BInfo(String bpid, Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0) {
        theBlock.setNumBytes(0);
      }
      if (!storage.alloc(bpid, theBlock.getNumBytes())) { 
        // expected length - actual length may
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
    
    synchronized void finalizeBlock(String bpid, long finalSize)
        throws IOException {
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
        if (!storage.alloc(bpid,extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        storage.free(bpid, -extraLen);
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
  
  /**
   * Class is used for tracking block pool storage utilization similar
   * to {@link BlockPoolSlice}
   */
  private static class SimulatedBPStorage {
    private long used;    // in bytes
    
    long getUsed() {
      return used;
    }
    
    void alloc(long amount) {
      used += amount;
    }
    
    void free(long amount) {
      used -= amount;
    }
    
    SimulatedBPStorage() {
      used = 0;   
    }
  }
  
  /**
   * Class used for tracking datanode level storage utilization similar
   * to {@link FSVolumeSet}
   */
  private static class SimulatedStorage {
    private Map<String, SimulatedBPStorage> map = 
      new HashMap<String, SimulatedBPStorage>();
    private long capacity;  // in bytes
    
    synchronized long getFree() {
      return capacity - getUsed();
    }
    
    synchronized long getCapacity() {
      return capacity;
    }
    
    synchronized long getUsed() {
      long used = 0;
      for (SimulatedBPStorage bpStorage : map.values()) {
        used += bpStorage.getUsed();
      }
      return used;
    }
    
    synchronized long getBlockPoolUsed(String bpid) throws IOException {
      return getBPStorage(bpid).getUsed();
    }
    
    int getNumFailedVolumes() {
      return 0;
    }

    synchronized boolean alloc(String bpid, long amount) throws IOException {
      if (getFree() >= amount) {
        getBPStorage(bpid).alloc(amount);
        return true;
      }
      return false;    
    }
    
    synchronized void free(String bpid, long amount) throws IOException {
      getBPStorage(bpid).free(amount);
    }
    
    SimulatedStorage(long cap) {
      capacity = cap;
    }
    
    synchronized void addBlockPool(String bpid) {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage != null) {
        return;
      }
      map.put(bpid, new SimulatedBPStorage());
    }
    
    synchronized void removeBlockPool(String bpid) {
      map.remove(bpid);
    }
    
    private SimulatedBPStorage getBPStorage(String bpid) throws IOException {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage == null) {
        throw new IOException("block pool " + bpid + " not found");
      }
      return bpStorage;
    }
  }
  
  private Map<String, Map<Block, BInfo>> blockMap = null;
  private SimulatedStorage storage = null;
  private String storageId;
  
  public SimulatedFSDataset(Configuration conf) throws IOException {
    setConf(conf);
  }
  
  // Constructor used for constructing the object using reflection
  @SuppressWarnings("unused")
  private SimulatedFSDataset() { // real construction when setConf called..
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
    blockMap = new HashMap<String, Map<Block,BInfo>>(); 
  }

  public synchronized void injectBlocks(String bpid,
      Iterable<Block> injectBlocks) throws IOException {
    ExtendedBlock blk = new ExtendedBlock();
    if (injectBlocks != null) {
      int numInjectedBlocks = 0;
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        numInjectedBlocks++;
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        blk.set(bpid, b);
        if (isValidBlock(blk)) {
          throw new IOException("Block already exists in  block list");
        }
      }
      Map<Block, BInfo> map = blockMap.get(bpid);
      if (map == null) {
        map = new HashMap<Block, BInfo>();
        blockMap.put(bpid, map);
      }
      
      for (Block b: injectBlocks) {
        BInfo binfo = new BInfo(bpid, b, false);
        map.put(binfo.theBlock, binfo);
      }
    }
  }
  
  /** Get a map for a given block pool Id */
  private Map<Block, BInfo> getMap(String bpid) throws IOException {
    final Map<Block, BInfo> map = blockMap.get(bpid);
    if (map == null) {
      throw new IOException("Non existent blockpool " + bpid);
    }
    return map;
  }

  @Override // FSDatasetInterface
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(b.getBlockPoolId(), b.getNumBytes());
  }

  @Override // FSDatasetInterface
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException {
    if (isValidRbw(b)) {
      blockMap.remove(b.getLocalBlock());
    }
  }

  @Override
  public synchronized BlockListAsLongs getBlockReport(String bpid) {
    final Map<Block, BInfo> map = blockMap.get(bpid);
    Block[] blockTable = new Block[map.size()];
    if (map != null) {
      int count = 0;
      for (BInfo b : map.values()) {
        if (b.isFinalized()) {
          blockTable[count++] = b.theBlock;
        }
      }
      if (count != blockTable.length) {
        blockTable = Arrays.copyOf(blockTable, count);
      }
    } else {
      blockTable = new Block[0];
    }
    return new BlockListAsLongs(
        new ArrayList<Block>(Arrays.asList(blockTable)), null);
  }

  @Override // FSDatasetMBean
  public long getCapacity() throws IOException {
    return storage.getCapacity();
  }

  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    return storage.getUsed();
  }

  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    return storage.getBlockPoolUsed(bpid);
  }
  
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    return storage.getFree();
  }

  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {
    return storage.getNumFailedVolumes();
  }

  @Override // FSDatasetInterface
  public synchronized long getLength(ExtendedBlock b) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    return binfo.getNumBytes();
  }

  @Override
  @Deprecated
  public Replica getReplica(String bpid, long blockId) {
    final Map<Block, BInfo> map = blockMap.get(bpid);
    if (map != null) {
      return map.get(new Block(blockId));
    }
    return null;
  }

  @Override 
  public synchronized String getReplicaString(String bpid, long blockId) {
    Replica r = null;
    final Map<Block, BInfo> map = blockMap.get(bpid);
    if (map != null) {
      r = map.get(new Block(blockId));
    }
    return r == null? "null": r.toString();
  }

  @Override // FSDatasetInterface
  public Block getStoredBlock(String bpid, long blkid) throws IOException {
    final Map<Block, BInfo> map = blockMap.get(bpid);
    if (map != null) {
      BInfo binfo = map.get(new Block(blkid));
      if (binfo == null) {
        return null;
      }
      return new Block(blkid, binfo.getGenerationStamp(), binfo.getNumBytes());
    }
    return null;
  }

  @Override // FSDatasetInterface
  public synchronized void invalidate(String bpid, Block[] invalidBlks)
      throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    final Map<Block, BInfo> map = getMap(bpid);
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      BInfo binfo = map.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      storage.free(bpid, binfo.getNumBytes());
      blockMap.remove(b);
    }
    if (error) {
      throw new IOException("Invalidate: Missing blocks.");
    }
  }

  @Override // FSDatasetInterface
  public synchronized boolean isValidBlock(ExtendedBlock b) {
    final Map<Block, BInfo> map = blockMap.get(b.getBlockPoolId());
    if (map == null) {
      return false;
    }
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      return false;
    }
    return binfo.isFinalized();
  }

  /* check if a block is created but not finalized */
  @Override
  public synchronized boolean isValidRbw(ExtendedBlock b) {
    final Map<Block, BInfo> map = blockMap.get(b.getBlockPoolId());
    if (map == null) {
      return false;
    }
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      return false;
    }
    return !binfo.isFinalized();  
  }

  @Override
  public String toString() {
    return getStorageInfo();
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface append(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null || !binfo.isFinalized()) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    binfo.unfinalizeBlock();
    return binfo;
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface recoverAppend(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      binfo.unfinalizeBlock();
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override // FSDatasetInterface
  public void recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
      throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (!binfo.isFinalized()) {
      binfo.finalizeBlock(b.getBlockPoolId(), binfo.getNumBytes());
    }
    map.remove(b.getLocalBlock());
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
  }
  
  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if ( binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " does not exist, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      throw new ReplicaAlreadyExistsException("Block " + b
          + " is valid, and cannot be written to.");
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface createRbw(ExtendedBlock b) 
  throws IOException {
    return createTemporary(b);
  }

  @Override // FSDatasetInterface
  public synchronized ReplicaInPipelineInterface createTemporary(ExtendedBlock b)
      throws IOException {
    if (isValidBlock(b)) {
          throw new ReplicaAlreadyExistsException("Block " + b + 
              " is valid, and cannot be written to.");
      }
    if (isValidRbw(b)) {
        throw new ReplicaAlreadyExistsException("Block " + b + 
            " is being written, and cannot be written to.");
    }
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = new BInfo(b.getBlockPoolId(), b.getLocalBlock(), true);
    map.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override // FSDatasetInterface
  public synchronized InputStream getBlockInputStream(ExtendedBlock b)
      throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    
    return binfo.getIStream();
  }
  
  @Override // FSDatasetInterface
  public synchronized InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    InputStream result = getBlockInputStream(b);
    result.skip(seekOffset);
    return result;
  }

  /** Not supported */
  @Override // FSDatasetInterface
  public BlockInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Returns metaData of block b as an input stream
   * @param b - the block for which the metadata is desired
   * @return metaData of block b as an input stream
   * @throws IOException - block does not exist or problems accessing
   *  the meta file
   */
  private synchronized InputStream getMetaDataInStream(ExtendedBlock b)
                                              throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b + 
          " is being written, its meta cannot be read");
    }
    return binfo.getMetaIStream();
  }
 
  @Override // FSDatasetInterface
  public synchronized long getMetaDataLength(ExtendedBlock b)
      throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its metalength cannot be read");
    }
    return binfo.getMetaIStream().getLength();
  }
  
  @Override // FSDatasetInterface
  public MetaDataInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
     return new MetaDataInputStream(getMetaDataInStream(b), 
                                    getMetaDataLength(b));
  }

  @Override // FSDatasetInterface
  public synchronized boolean metaFileExists(ExtendedBlock b) throws IOException {
    if (!isValidBlock(b)) {
          throw new IOException("Block " + b +
              " is valid, and cannot be written to.");
      }
    return true; // crc exists for all valid blocks
  }

  public void checkDataDir() throws DiskErrorException {
    // nothing to check for simulated data set
  }

  @Override // FSDatasetInterface
  public synchronized void adjustCrcChannelPosition(ExtendedBlock b,
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
      mbeanName = MBeans.register("DataNode", "FSDatasetState-"+
                                  storageId, bean);
    } catch (NotCompliantMBeanException e) {
      DataNode.LOG.warn("Error registering FSDatasetState MBean", e);
    }
 
    DataNode.LOG.info("Registered FSDatasetState MBean");
  }

  public void shutdown() {
    if (mbeanName != null) MBeans.unregister(mbeanName);
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
    ExtendedBlock b = rBlock.getBlock();
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }

    return new ReplicaRecoveryInfo(binfo.getBlockId(), binfo.getBytesOnDisk(), 
        binfo.getGenerationStamp(), 
        binfo.isFinalized()?ReplicaState.FINALIZED : ReplicaState.RBW);
  }

  @Override // FSDatasetInterface
  public FinalizedReplica updateReplicaUnderRecovery(ExtendedBlock oldBlock,
                                        long recoveryId,
                                        long newlength) throws IOException {
    return new FinalizedReplica(
        oldBlock.getBlockId(), newlength, recoveryId, null, null);
  }

  @Override // FSDatasetInterface
  public long getReplicaVisibleLength(ExtendedBlock block) throws IOException {
    return block.getNumBytes();
  }

  @Override // FSDatasetInterface
  public void addBlockPool(String bpid, Configuration conf) {
    Map<Block, BInfo> map = new HashMap<Block, BInfo>();
    blockMap.put(bpid, map);
    storage.addBlockPool(bpid);
  }
  
  @Override // FSDatasetInterface
  public void shutdownBlockPool(String bpid) {
    blockMap.remove(bpid);
    storage.removeBlockPool(bpid);
  }
  
  @Override // FSDatasetInterface
  public void deleteBlockPool(String bpid, boolean force) {
     return;
  }

  @Override
  public ReplicaInPipelineInterface convertTemporaryToRbw(ExtendedBlock temporary)
      throws IOException {
    final Map<Block, BInfo> map = blockMap.get(temporary.getBlockPoolId());
    if (map == null) {
      throw new IOException("Block pool not found, temporary=" + temporary);
    }
    final BInfo r = map.get(temporary.getLocalBlock());
    if (r == null) {
      throw new IOException("Block not found, temporary=" + temporary);
    } else if (r.isFinalized()) {
      throw new IOException("Replica already finalized, temporary="
          + temporary + ", r=" + r);
    }
    return r;
  }
}
