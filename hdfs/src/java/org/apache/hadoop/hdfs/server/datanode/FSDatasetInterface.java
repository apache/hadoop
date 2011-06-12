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


import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * This is an interface for the underlying storage that stores blocks for
 * a data node. 
 * Examples are the FSDataset (which stores blocks on dirs)  and 
 * SimulatedFSDataset (which simulates data).
 *
 */
@InterfaceAudience.Private
public interface FSDatasetInterface extends FSDatasetMBean {
  
  
  /**
   * Returns the length of the metadata file of the specified block
   * @param b - the block for which the metadata length is desired
   * @return the length of the metadata file for the specified block.
   * @throws IOException
   */
  public long getMetaDataLength(Block b) throws IOException;
  
  /**
   * This class provides the input stream and length of the metadata
   * of a block
   *
   */
  static class MetaDataInputStream extends FilterInputStream {
    MetaDataInputStream(InputStream stream, long len) {
      super(stream);
      length = len;
    }
    private long length;
    
    public long getLength() {
      return length;
    }
  }
  
  /**
   * Returns metaData of block b as an input stream (and its length)
   * @param b - the block
   * @return the metadata input stream; 
   * @throws IOException
   */
  public MetaDataInputStream getMetaDataInputStream(Block b)
        throws IOException;
  
  /**
   * Does the meta file exist for this block?
   * @param b - the block
   * @return true of the metafile for specified block exits
   * @throws IOException
   */
  public boolean metaFileExists(Block b) throws IOException;


  /**
   * Returns the specified block's on-disk length (excluding metadata)
   * @param b
   * @return   the specified block's on-disk length (excluding metadta)
   * @throws IOException
   */
  public long getLength(Block b) throws IOException;

  /**
   * Get reference to the replica meta info in the replicasMap. 
   * To be called from methods that are synchronized on {@link FSDataset}
   * @param blockId
   * @return replica from the replicas map
   */
  @Deprecated
  public Replica getReplica(long blockId);

  /**
   * @return the generation stamp stored with the block.
   */
  public Block getStoredBlock(long blkid) throws IOException;

  /**
   * Returns an input stream to read the contents of the specified block
   * @param b
   * @return an input stream to read the contents of the specified block
   * @throws IOException
   */
  public InputStream getBlockInputStream(Block b) throws IOException;
  
  /**
   * Returns an input stream at specified offset of the specified block
   * @param b
   * @param seekOffset
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public InputStream getBlockInputStream(Block b, long seekOffset)
            throws IOException;

  /**
   * Returns an input stream at specified offset of the specified block
   * The block is still in the tmp directory and is not finalized
   * @param b
   * @param blkoff
   * @param ckoff
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public BlockInputStreams getTmpInputStreams(Block b, long blkoff, long ckoff)
            throws IOException;

     /**
      * 
      * This class contains the output streams for the data and checksum
      * of a block
      *
      */
     static class BlockWriteStreams {
      OutputStream dataOut;
      OutputStream checksumOut;
      BlockWriteStreams(OutputStream dOut, OutputStream cOut) {
        dataOut = dOut;
        checksumOut = cOut;
      }
      
      void close() throws IOException {
        IOUtils.closeStream(dataOut);
        IOUtils.closeStream(checksumOut);
      }
    }

  /**
   * This class contains the input streams for the data and checksum
   * of a block
   */
  static class BlockInputStreams implements Closeable {
    final InputStream dataIn;
    final InputStream checksumIn;

    BlockInputStreams(InputStream dataIn, InputStream checksumIn) {
      this.dataIn = dataIn;
      this.checksumIn = checksumIn;
    }

    /** {@inheritDoc} */
    public void close() {
      IOUtils.closeStream(dataIn);
      IOUtils.closeStream(checksumIn);
    }
  }
    
  /**
   * Creates a temporary replica and returns the meta information of the replica
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface createTemporary(Block b)
  throws IOException;

  /**
   * Creates a RBW replica and returns the meta info of the replica
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface createRbw(Block b) throws IOException;

  /**
   * Recovers a RBW replica and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param minBytesRcvd the minimum number of bytes that the replica could have
   * @param maxBytesRcvd the maximum number of bytes that the replica could have
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface recoverRbw(Block b, 
      long newGS, long minBytesRcvd, long maxBytesRcvd)
  throws IOException;

  /**
   * Append to a finalized replica and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meata info of the replica which is being written to
   * @throws IOException
   */
  public ReplicaInPipelineInterface append(Block b, 
      long newGS, long expectedBlockLen) throws IOException;

  /**
   * Recover a failed append to a finalized replica
   * and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meta info of the replica which is being written to
   * @throws IOException
   */
  public ReplicaInPipelineInterface recoverAppend(Block b,
      long newGS, long expectedBlockLen) throws IOException;
  
  /**
   * Recover a failed pipeline close
   * It bumps the replica's generation stamp and finalize it if RBW replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @throws IOException
   */
  public void recoverClose(Block b,
      long newGS, long expectedBlockLen) throws IOException;
  
  /**
   * Finalizes the block previously opened for writing using writeToBlock.
   * The block size is what is in the parameter b and it must match the amount
   *  of data written
   * @param b
   * @throws IOException
   */
  public void finalizeBlock(Block b) throws IOException;

  /**
   * Unfinalizes the block previously opened for writing using writeToBlock.
   * The temporary file associated with this block is deleted.
   * @param b
   * @throws IOException
   */
  public void unfinalizeBlock(Block b) throws IOException;

  /**
   * Returns the block report - the full list of blocks stored
   * @return - the block report - the full list of blocks stored
   */
  public BlockListAsLongs getBlockReport();

  /**
   * Is the block valid?
   * @param b
   * @return - true if the specified block is valid
   */
  public boolean isValidBlock(Block b);

  /**
   * Invalidates the specified blocks
   * @param invalidBlks - the blocks to be invalidated
   * @throws IOException
   */
  public void invalidate(Block invalidBlks[]) throws IOException;

    /**
     * Check if all the data directories are healthy
     * @throws DiskErrorException
     */
  public void checkDataDir() throws DiskErrorException;
      
    /**
     * Stringifies the name of the storage
     */
  public String toString();
  
  /**
   * Shutdown the FSDataset
   */
  public void shutdown();

  /**
   * Sets the file pointer of the checksum stream so that the last checksum
   * will be overwritten
   * @param b block
   * @param stream The stream for the data file and checksum file
   * @param checksumSize number of bytes each checksum has
   * @throws IOException
   */
  public void adjustCrcChannelPosition(Block b, BlockWriteStreams stream, 
      int checksumSize) throws IOException;

  /**
   * checks how many valid storage volumes are there in the DataNode
   * @return true if more then minimum valid volumes left in the FSDataSet
   */
  public boolean hasEnoughResource();

  /**
   * Get visible length of the specified replica.
   */
  long getReplicaVisibleLength(final Block block) throws IOException;

  /**
   * Initialize a replica recovery.
   * 
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   */
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException;

  /**
   * Update replica's generation stamp and length and finalize it.
   */
  public ReplicaInfo updateReplicaUnderRecovery(
                                          Block oldBlock,
                                          long recoveryId,
                                          long newLength) throws IOException;
}
