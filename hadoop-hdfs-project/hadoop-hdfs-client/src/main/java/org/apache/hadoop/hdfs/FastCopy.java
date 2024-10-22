/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_BLOCK_EXECUTOR_POOLSIZE;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_BLOCK_EXECUTOR_POOLSIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_BLOCK_WAIT_TIME_MS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_BLOCK_WAIT_TIME_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_FILE_WAIT_TIME_MS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_FAST_COPY_FILE_WAIT_TIME_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;

/**
 * There is a need to perform fast file copy on HDFS (primarily for the purpose
 * of HBase Snapshot). The fast copy mechanism for a file works as follows :
 * <p>
 * 1) Query metadata for all blocks of the source file.
 * <p>
 * 2) For each block 'b' of the file, find out its datanode locations.
 * <p>
 * 3) For each block of the file, add an empty block to the nameSystem for the
 * destination file.
 * <p>
 * 4) For each location of the block, instruct the datanode to make a local copy
 * of that block.
 * <p>
 * 5) Once each datanode has copied over its respective blocks, they report
 * to the nameNode about it.
 * <p>
 * 6) Wait for all blocks to be copied and exit.
 * <p>
 * This would speed up the copying process considerably by removing top of the
 * rack data transfers.
 **/

public class FastCopy {
  public static final Logger LOG = LoggerFactory.getLogger(FastCopy.class);
  protected Configuration conf;
  // Map used to store the status of each block.
  private final Map<ExtendedBlock, BlockStatus> blockStatusMap = new ConcurrentHashMap<>();
  private final FastCopyFileStatus fileStatus;
  private final short minReplication;
  // The time for which to wait for a block to be reported to the nameNode.
  private final long BLK_WAIT_TIME;
  // Maximum time to wait for a file copy to complete.
  public final long FILE_WAIT_TIME;
  private final EnumSet<CreateFlag> flag;

  private final String src;
  private final String dst;
  private final ExecutorService copyBlockExecutor;
  private volatile IOException copyBlockException = null;
  public int copyBlockExecutorPoolSize;

  private final DFSClient srcDFSClient;
  private final DistributedFileSystem dstFs;
  private long chunkOffset = 0;
  private long chunkLength = Long.MAX_VALUE;


  public FastCopy(Configuration conf, Path sourcePath, Path dstPath, boolean overwrite) throws IOException {
    this.conf = conf;
    FILE_WAIT_TIME =
        conf.getInt(DFS_FAST_COPY_FILE_WAIT_TIME_MS, DFS_FAST_COPY_FILE_WAIT_TIME_MS_DEFAULT);
    BLK_WAIT_TIME =
        conf.getInt(DFS_FAST_COPY_BLOCK_WAIT_TIME_MS, DFS_FAST_COPY_BLOCK_WAIT_TIME_MS_DEFAULT);
    minReplication = (short) conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY, 1);

    if (overwrite) {
      flag = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    } else {
      flag = EnumSet.of(CreateFlag.CREATE);
    }

    DistributedFileSystem srcFs = (DistributedFileSystem) sourcePath.getFileSystem(conf);
    this.dstFs = (DistributedFileSystem) dstPath.getFileSystem(conf);
    this.srcDFSClient = srcFs.getClient();
    this.src =
        sourcePath.makeQualified(srcFs.getUri(), srcFs.getWorkingDirectory()).toUri().getPath();
    this.dst =
        dstPath.makeQualified(dstFs.getUri(), dstFs.getWorkingDirectory()).toUri().getPath();
    this.fileStatus = new FastCopyFileStatus(this.dst);
    // This controls the number of concurrent blocks that would be copied per
    // file. So if we are concurrently copying 5 files at a time by setting
    // THREAD_POOL_SIZE to 5 and allowing 5 concurrent file copies and this
    // value is set at 5, we would have 25 blocks in all being copied by the
    // tool in parallel.
    this.copyBlockExecutorPoolSize = conf.getInt(DFS_FAST_COPY_BLOCK_EXECUTOR_POOLSIZE,
        DFS_FAST_COPY_BLOCK_EXECUTOR_POOLSIZE_DEFAULT);
    this.copyBlockExecutor = HadoopExecutors.newFixedThreadPool(this.copyBlockExecutorPoolSize);
  }

  public FastCopy(Configuration conf, Path sourcePath, Path dstPath, boolean overwrite, long chunkOffset, long chunkLength) throws IOException {
     this(conf,sourcePath,dstPath,overwrite);
     this.chunkOffset = chunkOffset;
     this.chunkLength = chunkLength;
  }

  private class CopyBlockCrossNamespace implements Runnable {
    private final ExtendedBlock source;
    private final ExtendedBlock target;
    private final DatanodeInfo sourceDn;
    private final DatanodeInfo targetDn;
    private final Token<BlockTokenIdentifier> sourceBlockToken;
    private final Token<BlockTokenIdentifier> targetToken;
    private final boolean isECFile;
    private final DFSOutputStream outputStream;

    public CopyBlockCrossNamespace(ExtendedBlock source, ExtendedBlock target,
        DatanodeInfo sourceDn, DatanodeInfo targetDn, Token<BlockTokenIdentifier> sourceBlockToken,
        Token<BlockTokenIdentifier> targetToken, boolean isECFile, DFSOutputStream outputStream) {
      this.source = source;
      this.target = target;
      this.sourceDn = sourceDn;
      this.targetDn = targetDn;
      this.sourceBlockToken = sourceBlockToken;
      this.targetToken = targetToken;
      this.isECFile = isECFile;
      this.outputStream = outputStream;
    }

    @Override
    public void run() {
      copyBlockReplica();
    }

    private void copyBlockReplica() {
      boolean isError = false;
      try {
        outputStream.copyBlockCrossNamespace(source, sourceBlockToken, sourceDn, target,
            targetToken, targetDn);
      } catch (IOException e) {
        String errMsg = "Fast Copy : Failed for Copying block " + source.getBlockName() + " from "
            + sourceDn.getInfoAddr() + " to " + target.getBlockName() + " on "
            + targetDn.getInfoAddr();
        LOG.warn(errMsg, e);
        isError = true;
      }
      updateBlockStatus(target, isError, isECFile);
    }

    /**
     * Updates the status of a block. If the block is in the
     * {@link FastCopy#blockStatusMap} we are still waiting for the block to
     * reach the desired replication level.
     *
     * @param b       the block whose status needs to be updated.
     * @param isError whether the block had an error.
     */
    private void updateBlockStatus(ExtendedBlock b, boolean isError, boolean isECFile) {
      if (isECFile) {
        b = new ExtendedBlock(b.getBlockPoolId(),
            b.getBlockId() - StripedBlockUtil.getBlockIndex(b.getLocalBlock()));
      }

      synchronized (blockStatusMap) {
        BlockStatus bStatus = blockStatusMap.get(b);
        if (bStatus == null) {
          return;
        }
        if (isError) {
          bStatus.addBadReplica();
          if (bStatus.isBadBlock()) {
            blockStatusMap.remove(b);
            copyBlockException =
                new IOException("All replicas are bad for block : " + b.getBlockName());
          }
        } else {
          bStatus.addGoodReplica();
          // We are removing the block from the blockStatusMap, this indicates
          // that the block has reached the desired replication, so now we
          // update the fileStatusMap. Note that this will happen only once
          // for each block.
          if (bStatus.isGoodBlock()) {
            blockStatusMap.remove(b);
            updateFileStatus();
          }
        }
      }
    }

    /**
     * Updates the file status by incrementing the total number of blocks done
     * for this file by 1.
     */
    private void updateFileStatus() {
      synchronized (fileStatus) {
        fileStatus.addBlock();
      }
    }
  }

  /**
   * Stores the status of a single block, the number of replicas that are bad
   * and the total number of expected replicas.
   */
  public class BlockStatus {
    private final short totalReplicas;
    private short badReplicas;
    private short goodReplicas;
    private final boolean isECFile;

    public BlockStatus(short totalReplicas, boolean isECFile) {
      this.totalReplicas = totalReplicas;
      this.badReplicas = 0;
      this.goodReplicas = 0;
      this.isECFile = isECFile;
    }

    public void addBadReplica() {
      this.badReplicas++;
    }

    public boolean isBadBlock() {
      return (badReplicas >= totalReplicas);
    }

    public void addGoodReplica() {
      this.goodReplicas++;
    }

    public boolean isGoodBlock() {
      if (isECFile) {
        return (this.goodReplicas >= totalReplicas);
      }
      return (this.goodReplicas >= minReplication);
    }
  }

  /**
   * This is used for status reporting by the Fast Copy tool
   */
  public static class FastCopyFileStatus {
    // The file that data is being copied to.
    private final String file;
    // The total number of blocks done till now.
    private int blocksDone;

    public FastCopyFileStatus(String file) {
      this(file, 0);
    }

    public FastCopyFileStatus(String file, int blocksDone) {
      this.file = file;
      this.blocksDone = blocksDone;
    }

    public String getFileName() {
      return this.file;
    }

    public int getBlocksDone() {
      return this.blocksDone;
    }

    public void addBlock() {
      this.blocksDone++;
    }

    public String toString() {
      return "Copying " + file + " has finished " + blocksDone + " blocks.";
    }
  }

  /**
   * Aligns the source and destination locations such that common locations
   * appear at the same index.
   *
   * @param dstLocations the destination dataNodes
   * @param srcLocations the source dataNodes
   */
  public void alignDataNodes(DatanodeInfo[] dstLocations, DatanodeInfo[] srcLocations) {
    for (int i = 0; i < dstLocations.length; i++) {
      for (int j = 0; j < srcLocations.length; j++) {
        if (i == j)
          continue;
        if (dstLocations[i].equals(srcLocations[j])) {
          if (i < j) {
            swap(i, j, srcLocations);
          } else {
            swap(i, j, dstLocations);
          }
          break;
        }
      }
    }
  }

  private void swap(int i, int j, DatanodeInfo[] arr) {
    DatanodeInfo tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }

  /**
   * Copies all the replicas for a single replicated block
   *
   * @param src the source block
   * @param dst the destination block
   */
  private void startCopyBlockThread(LocatedBlock src, LocatedBlock dst, DFSOutputStream out) {
    // Sorting source and destination locations so that we don't rely at all
    // on the ordering of the locations that we receive from the NameNode.
    DatanodeInfo[] dstLocations = dst.getLocations();
    DatanodeInfo[] srcLocations = src.getLocations();
    alignDataNodes(dstLocations, srcLocations);

    // We use minimum here, since its better for the NameNode to handle the
    // extra locations in either list. The locations that match up are the
    // ones we have chosen in our tool, so we handle copies for only those.
    short blocksToCopy = (short) Math.min(srcLocations.length, dstLocations.length);

    ExtendedBlock srcBlock = src.getBlock();
    ExtendedBlock dstBlock = dst.getBlock();
    initializeBlockStatus(dstBlock, blocksToCopy, false);
    for (int i = 0; i < blocksToCopy; i++) {
      copyBlockExecutor.submit(
          new CopyBlockCrossNamespace(srcBlock, dstBlock, srcLocations[i], dstLocations[i],
              src.getBlockToken(), dst.getBlockToken(), false, out));
    }
  }

  private void startCopyBlockThread(LocatedBlock srcLocatedBlock,
      LocatedBlock destinationLocatedBlock, ErasureCodingPolicy erasureCodingPolicy,
      DFSOutputStream out) {
    LocatedBlock[] srcBlocks =
        StripedBlockUtil.parseStripedBlockGroup((LocatedStripedBlock) srcLocatedBlock,
            erasureCodingPolicy.getCellSize(), erasureCodingPolicy.getNumDataUnits(),
            erasureCodingPolicy.getNumParityUnits());

    LocatedBlock[] dstBlocks =
        StripedBlockUtil.parseStripedBlockGroup((LocatedStripedBlock) destinationLocatedBlock,
            erasureCodingPolicy.getCellSize(), erasureCodingPolicy.getNumDataUnits(),
            erasureCodingPolicy.getNumParityUnits());

    short notEmptyBlocks = 0;
    for (LocatedBlock s : srcBlocks) {
      if (s != null) {
        notEmptyBlocks++;
      }
    }

    initializeBlockStatus(destinationLocatedBlock.getBlock(), notEmptyBlocks, true);

    for (int i = 0; i < srcBlocks.length; i++) {
      if (srcBlocks[i] != null) {
        copyBlockExecutor.submit(
            new CopyBlockCrossNamespace(srcBlocks[i].getBlock(), dstBlocks[i].getBlock(),
                srcBlocks[i].getLocations()[0], dstBlocks[i].getLocations()[0],
                srcBlocks[i].getBlockToken(), dstBlocks[i].getBlockToken(), true, out));
      }
    }
  }

  /**
   * Waits for the blocks of the file to be completed to a particular
   * threshold.
   *
   * @param blocksAdded the number of blocks already added to the nameNode.
   * @throws IOException throw IOException if timeout or copyBlockException has exception.
   */
  private void waitForBlockCopy(int blocksAdded) throws IOException {
    long startTime = Time.monotonicNow();

    while (true) {
      // If the dataNodes are not lagging or this is the first block that will
      // be added to the nameNode, no need to wait longer.
      int blocksDone = fileStatus.getBlocksDone();
      if (blocksAdded == blocksDone || blocksAdded == 0) {
        break;
      }
      if (copyBlockException != null) {
        throw copyBlockException;
      }
      if (Time.monotonicNow() - startTime > BLK_WAIT_TIME) {
        throw new IOException("Timeout waiting for block to be copied.");
      }
      sleepFor(100);
    }
  }

  /**
   * Initializes the block status map with information about a block.
   *
   * @param b             the block to be added to the {@link FastCopy#blockStatusMap}
   * @param totalReplicas the number of replicas for b
   */
  private void initializeBlockStatus(ExtendedBlock b, short totalReplicas, boolean isECFile) {
    BlockStatus bStatus = new BlockStatus(totalReplicas, isECFile);
    blockStatusMap.put(b, bStatus);
  }

  /**
   * Shuts down the block rpc executor.
   */
  private void terminateExecutor() throws IOException {
    copyBlockExecutor.shutdown();
    try {
      copyBlockExecutor.awaitTermination(FILE_WAIT_TIME, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
  }

  /**
   * Sleeps the current thread for <code>ms</code> milliseconds.
   *
   * @param ms the number of milliseconds to sleep the current thread
   * @throws IOException if it encountered an InterruptedException
   */
  private void sleepFor(long ms) throws IOException {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Copy the file.
   */
  public void copyFile() throws IOException {
    // Get source file information and create empty destination file.
    HdfsFileStatus srcFileStatus = srcDFSClient.getFileInfo(src);
    if (srcFileStatus == null) {
      throw new FileNotFoundException("File : " + src + " does not exist");
    }

    LOG.info("Start to copy {} to {}.", src, dst);
    try {
      LocatedBlocks blocks = srcDFSClient.getLocatedBlocks(src, chunkOffset, chunkLength);
      List<LocatedBlock> blocksList = blocks.getLocatedBlocks();
      LOG.debug("FastCopy : Block locations retrieved for {} : {}.", src, blocksList);

      ErasureCodingPolicy erasureCodingPolicy = srcFileStatus.getErasureCodingPolicy();
      HdfsDataOutputStream stream =
          dstFs.create(new Path(dst), srcFileStatus.getPermission(), flag,
              dstFs.getClient().getConf().getIoBufferSize(), srcFileStatus.getReplication(),
              srcFileStatus.getBlockSize(), null, null, null,
              erasureCodingPolicy == null ? null : erasureCodingPolicy.getName(), null);
      DFSOutputStream out = (DFSOutputStream) stream.getWrappedStream();

      // Instruct each datanode to create a copy of the respective block.
      int blocksAdded = 0;
      ExtendedBlock previous = null;
      LocatedBlock dstLocatedBlock;
      // Loop through each block and create copies.
      for (LocatedBlock srcLocatedBlock : blocksList) {
        UserGroupInformation.getCurrentUser().addToken(srcLocatedBlock.getBlockToken());
        dstLocatedBlock = erasureCodingPolicy == null ?
            copyBlocks(previous, srcLocatedBlock, out) :
            copyBlocks(erasureCodingPolicy, previous, srcLocatedBlock, out);

        blocksAdded++;
        // Wait for the block copies to reach a threshold.
        waitForBlockCopy(blocksAdded);

        previous = dstLocatedBlock.getBlock();
        previous.setNumBytes(srcLocatedBlock.getBlockSize());
      }
      out.setUserAssignmentLastBlock(previous);
      stream.close();
    } catch (IOException e) {
      LOG.error("Failed to copy src:{} to dst: {} .", src, dst);
      throw new IOException(e);
    } finally {
      terminateExecutor();
    }
  }

  private LocatedBlock copyBlocks(ExtendedBlock previous, LocatedBlock srcLocatedBlock,
      DFSOutputStream out) throws IOException {
    String[] favoredNodes = new String[srcLocatedBlock.getLocations().length];
    for (int i = 0; i < srcLocatedBlock.getLocations().length; i++) {
      favoredNodes[i] = srcLocatedBlock.getLocations()[i].getHostName() + ":"
          + srcLocatedBlock.getLocations()[i].getXferPort();
    }
    LOG.debug("FavoredNodes for {}: {}.", srcLocatedBlock, Arrays.toString(favoredNodes));

    LocatedBlock dstLocatedBlock =
        DFSOutputStream.addBlock(null, out.getDfsClient(), dst, previous, out.getFileId(),
            favoredNodes, out.getAddBlockFlags());
    if (dstLocatedBlock == null) {
      throw new IOException(dst + " get null located block from dst nameNode.");
    }
    //blocksAdded++;
    LOG.debug("Fast Copy : Block {} added to {}.", dstLocatedBlock.getBlock(), dst);
    startCopyBlockThread(srcLocatedBlock, dstLocatedBlock, out);
    return dstLocatedBlock;
  }

  private LocatedBlock copyBlocks(ErasureCodingPolicy erasureCodingPolicy, ExtendedBlock previous,
      LocatedBlock srcLocatedBlock, DFSOutputStream out) throws IOException {
    LocatedBlock[] srcBlocks =
        StripedBlockUtil.parseStripedBlockGroup((LocatedStripedBlock) srcLocatedBlock,
            erasureCodingPolicy.getCellSize(), erasureCodingPolicy.getNumDataUnits(),
            erasureCodingPolicy.getNumParityUnits());
    String[] favoredNodes = new String[srcBlocks.length];

    for (int i = 0; i < srcBlocks.length; i++) {
      if (srcBlocks[i] != null) {
        favoredNodes[i] = srcBlocks[i].getLocations()[0].getHostName() + ":"
            + srcBlocks[i].getLocations()[0].getXferPort();
      } else {
        favoredNodes[i] = "";
      }
    }
    LOG.debug("FavoredNodes for {}: {}.", srcLocatedBlock, Arrays.toString(favoredNodes));
    LocatedBlock dstLocatedBlock =
        DFSOutputStream.addBlock(null, out.getDfsClient(), dst, previous, out.getFileId(),
            favoredNodes, out.getAddBlockFlags());
    if (dstLocatedBlock == null) {
      throw new IOException(dst + " get null located block from dst nameNode.");
    }
    //blocksAdded++;
    LOG.debug("Fast Copy : Block {} added to {}.", dstLocatedBlock.getBlock(), dst);
    startCopyBlockThread(srcLocatedBlock, dstLocatedBlock, erasureCodingPolicy, out);
    return dstLocatedBlock;
  }
}