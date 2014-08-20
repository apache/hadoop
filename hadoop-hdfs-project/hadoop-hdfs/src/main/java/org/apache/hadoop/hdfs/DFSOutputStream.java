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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;


/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer
    implements Syncable, CanSetDropBehind {
  private static final int MAX_PACKETS = 80; // each packet 64K, total 5MB
  private final DFSClient dfsClient;
  private final long dfsclientSlowLogThresholdMs;
  private Socket s;
  // closed is accessed by different threads under different locks.
  private volatile boolean closed = false;

  private String src;
  private final long fileId;
  private final long blockSize;
  private final DataChecksum checksum;
  // both dataQueue and ackQueue are protected by dataQueue lock
  private final LinkedList<Packet> dataQueue = new LinkedList<Packet>();
  private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
  private Packet currentPacket = null;
  private DataStreamer streamer;
  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private int packetSize = 0; // write packet size, not including the header.
  private int chunksPerPacket = 0;
  private final AtomicReference<IOException> lastException = new AtomicReference<IOException>();
  private long artificialSlowdown = 0;
  private long lastFlushOffset = 0; // offset when flush was invoked
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private volatile boolean appendChunk = false;   // appending to existing partial block
  private long initialFileSize = 0; // at time of file open
  private final Progressable progress;
  private final short blockReplication; // replication factor of file
  private boolean shouldSyncBlock = false; // force blocks to disk upon close
  private final AtomicReference<CachingStrategy> cachingStrategy;
  private boolean failPacket = false;
  
  private static class Packet {
    private static final long HEART_BEAT_SEQNO = -1L;
    final long seqno; // sequencenumber of buffer in block
    final long offsetInBlock; // offset in block
    boolean syncBlock; // this packet forces the current block to disk
    int numChunks; // number of chunks currently in packet
    final int maxChunks; // max chunks in packet
    final byte[]  buf;
    private boolean lastPacketInBlock; // is this the last packet in block?

    /**
     * buf is pointed into like follows:
     *  (C is checksum data, D is payload data)
     *
     * [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
     *           ^        ^               ^               ^
     *           |        checksumPos     dataStart       dataPos
     *           checksumStart
     * 
     * Right before sending, we move the checksum data to immediately precede
     * the actual data, and then insert the header into the buffer immediately
     * preceding the checksum data, so we make sure to keep enough space in
     * front of the checksum data to support the largest conceivable header. 
     */
    int checksumStart;
    int checksumPos;
    final int dataStart;
    int dataPos;

    /**
     * Create a heartbeat packet.
     */
    Packet(int checksumSize) {
      this(0, 0, 0, HEART_BEAT_SEQNO, checksumSize);
    }
    
    /**
     * Create a new packet.
     * 
     * @param pktSize maximum size of the packet, 
     *                including checksum data and actual data.
     * @param chunksPerPkt maximum number of chunks per packet.
     * @param offsetInBlock offset in bytes into the HDFS block.
     */
    Packet(int pktSize, int chunksPerPkt, long offsetInBlock, 
                              long seqno, int checksumSize) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = seqno;
      
      buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN + pktSize];
      
      checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
      checksumPos = checksumStart;
      dataStart = checksumStart + (chunksPerPkt * checksumSize);
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    void writeData(byte[] inarray, int off, int len) {
      if (dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void writeChecksum(byte[] inarray, int off, int len) {
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }
    
    /**
     * Write the full packet, including the header, to the given output stream.
     */
    void writeTo(DataOutputStream stm) throws IOException {
      final int dataLen = dataPos - dataStart;
      final int checksumLen = checksumPos - checksumStart;
      final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;

      PacketHeader header = new PacketHeader(
        pktLen, offsetInBlock, seqno, lastPacketInBlock, dataLen, syncBlock);
      
      if (checksumPos != dataStart) {
        // Move the checksum to cover the gap. This can happen for the last
        // packet or during an hflush/hsync call.
        System.arraycopy(buf, checksumStart, buf, 
                         dataStart - checksumLen , checksumLen); 
        checksumPos = dataStart;
        checksumStart = checksumPos - checksumLen;
      }
      
      final int headerStart = checksumStart - header.getSerializedSize();
      assert checksumStart + 1 >= header.getSerializedSize();
      assert checksumPos == dataStart;
      assert headerStart >= 0;
      assert headerStart + header.getSerializedSize() == checksumStart;
      
      // Copy the header data into the buffer immediately preceding the checksum
      // data.
      System.arraycopy(header.getBytes(), 0, buf, headerStart,
          header.getSerializedSize());
      
      // corrupt the data for testing.
      if (DFSClientFaultInjector.get().corruptPacket()) {
        buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
      }

      // Write the now contiguous full packet to the output stream.
      stm.write(buf, headerStart, header.getSerializedSize() + checksumLen + dataLen);

      // undo corruption.
      if (DFSClientFaultInjector.get().uncorruptPacket()) {
        buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
      }
    }
    
    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }
    
    /**
     * Check if this packet is a heart beat packet
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }
    
    @Override
    public String toString() {
      return "packet seqno:" + this.seqno +
      " offsetInBlock:" + this.offsetInBlock + 
      " lastPacketInBlock:" + this.lastPacketInBlock +
      " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
    }
  }

  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  //
  class DataStreamer extends Daemon {
    private volatile boolean streamerClosed = false;
    private ExtendedBlock block; // its length is number of bytes acked
    private Token<BlockTokenIdentifier> accessToken;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private ResponseProcessor response = null;
    private volatile DatanodeInfo[] nodes = null; // list of targets for current block
    private volatile StorageType[] storageTypes = null;
    private volatile String[] storageIDs = null;
    private final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes =
        CacheBuilder.newBuilder()
        .expireAfterWrite(
            dfsClient.getConf().excludedNodesCacheExpiry,
            TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              RemovalNotification<DatanodeInfo, DatanodeInfo> notification) {
            DFSClient.LOG.info("Removing node " +
                notification.getKey() + " from the excluded nodes list");
          }
        })
        .build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
    private String[] favoredNodes;
    volatile boolean hasError = false;
    volatile int errorIndex = -1;
    volatile int restartingNodeIndex = -1; // Restarting node index
    private long restartDeadline = 0; // Deadline of DN restart
    private BlockConstructionStage stage;  // block construction stage
    private long bytesSent = 0; // number of bytes that've been sent

    /** Nodes have been used in the pipeline before and have failed. */
    private final List<DatanodeInfo> failed = new ArrayList<DatanodeInfo>();
    /** The last ack sequence number before pipeline failure. */
    private long lastAckedSeqnoBeforeFailure = -1;
    private int pipelineRecoveryCount = 0;
    /** Has the current block been hflushed? */
    private boolean isHflushed = false;
    /** Append on an existing block? */
    private final boolean isAppend;

    /**
     * Default construction for file create
     */
    private DataStreamer() {
      isAppend = false;
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }
    
    /**
     * Construct a data streamer for append
     * @param lastBlock last block of the file to be appended
     * @param stat status of the file to be appended
     * @param bytesPerChecksum number of bytes per checksum
     * @throws IOException if error occurs
     */
    private DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
        int bytesPerChecksum) throws IOException {
      isAppend = true;
      stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
      block = lastBlock.getBlock();
      bytesSent = block.getNumBytes();
      accessToken = lastBlock.getBlockToken();
      long usedInLastBlock = stat.getLen() % blockSize;
      int freeInLastBlock = (int)(blockSize - usedInLastBlock);

      // calculate the amount of free space in the pre-existing 
      // last crc chunk
      int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
      int freeInCksum = bytesPerChecksum - usedInCksum;

      // if there is space in the last block, then we have to 
      // append to that block
      if (freeInLastBlock == blockSize) {
        throw new IOException("The last block for file " + 
            src + " is full.");
      }

      if (usedInCksum > 0 && freeInCksum > 0) {
        // if there is space in the last partial chunk, then 
        // setup in such a way that the next packet will have only 
        // one chunk that fills up the partial chunk.
        //
        computePacketChunkSize(0, freeInCksum);
        resetChecksumChunk(freeInCksum);
        appendChunk = true;
      } else {
        // if the remaining space in the block is smaller than 
        // that expected size of of a packet, then create 
        // smaller size packet.
        //
        computePacketChunkSize(Math.min(dfsClient.getConf().writePacketSize, freeInLastBlock), 
            bytesPerChecksum);
      }

      // setup pipeline to append to the last block XXX retries??
      setPipeline(lastBlock);
      errorIndex = -1;   // no errors yet.
      if (nodes.length < 1) {
        throw new IOException("Unable to retrieve blocks locations " +
            " for last block " + block +
            "of file " + src);

      }
    }
    
    private void setPipeline(LocatedBlock lb) {
      setPipeline(lb.getLocations(), lb.getStorageTypes(), lb.getStorageIDs());
    }
    private void setPipeline(DatanodeInfo[] nodes, StorageType[] storageTypes,
        String[] storageIDs) {
      this.nodes = nodes;
      this.storageTypes = storageTypes;
      this.storageIDs = storageIDs;
    }

    private void setFavoredNodes(String[] favoredNodes) {
      this.favoredNodes = favoredNodes;
    }

    /**
     * Initialize for data streaming
     */
    private void initDataStreaming() {
      this.setName("DataStreamer for file " + src +
          " block " + block);
      response = new ResponseProcessor(nodes);
      response.start();
      stage = BlockConstructionStage.DATA_STREAMING;
    }
    
    private void endBlock() {
      if(DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Closing old block " + block);
      }
      this.setName("DataStreamer for file " + src);
      closeResponder();
      closeStream();
      setPipeline(null, null, null);
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }
    
    /*
     * streamer thread is the only thread that opens streams to datanode, 
     * and closes them. Any error recovery is also done by this thread.
     */
    @Override
    public void run() {
      long lastPacket = Time.now();
      while (!streamerClosed && dfsClient.clientRunning) {

        // if the Responder encountered an error, shutdown Responder
        if (hasError && response != null) {
          try {
            response.close();
            response.join();
            response = null;
          } catch (InterruptedException  e) {
            DFSClient.LOG.warn("Caught exception ", e);
          }
        }

        Packet one;
        try {
          // process datanode IO errors if any
          boolean doSleep = false;
          if (hasError && (errorIndex >= 0 || restartingNodeIndex >= 0)) {
            doSleep = processDatanodeError();
          }

          synchronized (dataQueue) {
            // wait for a packet to be sent.
            long now = Time.now();
            while ((!streamerClosed && !hasError && dfsClient.clientRunning 
                && dataQueue.size() == 0 && 
                (stage != BlockConstructionStage.DATA_STREAMING || 
                 stage == BlockConstructionStage.DATA_STREAMING && 
                 now - lastPacket < dfsClient.getConf().socketTimeout/2)) || doSleep ) {
              long timeout = dfsClient.getConf().socketTimeout/2 - (now-lastPacket);
              timeout = timeout <= 0 ? 1000 : timeout;
              timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                 timeout : 1000;
              try {
                dataQueue.wait(timeout);
              } catch (InterruptedException  e) {
                DFSClient.LOG.warn("Caught exception ", e);
              }
              doSleep = false;
              now = Time.now();
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            // get packet to be sent.
            if (dataQueue.isEmpty()) {
              one = new Packet(checksum.getChecksumSize());  // heartbeat packet
            } else {
              one = dataQueue.getFirst(); // regular data packet
            }
          }
          assert one != null;

          // get new block from namenode.
          if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
            if(DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Allocating new block");
            }
            setPipeline(nextBlockOutputStream());
            initDataStreaming();
          } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
            if(DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Append to block " + block);
            }
            setupPipelineForAppendOrRecovery();
            initDataStreaming();
          }

          long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
          if (lastByteOffsetInBlock > blockSize) {
            throw new IOException("BlockSize " + blockSize +
                " is smaller than data size. " +
                " Offset of packet in block " + 
                lastByteOffsetInBlock +
                " Aborting file " + src);
          }

          if (one.lastPacketInBlock) {
            // wait for all data packets have been successfully acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                try {
                  // wait for acks to arrive from datanodes
                  dataQueue.wait(1000);
                } catch (InterruptedException  e) {
                  DFSClient.LOG.warn("Caught exception ", e);
                }
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            stage = BlockConstructionStage.PIPELINE_CLOSE;
          }
          
          // send the packet
          synchronized (dataQueue) {
            // move packet from dataQueue to ackQueue
            if (!one.isHeartbeatPacket()) {
              dataQueue.removeFirst();
              ackQueue.addLast(one);
              dataQueue.notifyAll();
            }
          }

          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("DataStreamer block " + block +
                " sending packet " + one);
          }

          // write out data to remote datanode
          try {
            one.writeTo(blockStream);
            blockStream.flush();   
          } catch (IOException e) {
            // HDFS-3398 treat primary DN is down since client is unable to 
            // write to primary DN. If a failed or restarting node has already
            // been recorded by the responder, the following call will have no 
            // effect. Pipeline recovery can handle only one node error at a
            // time. If the primary node fails again during the recovery, it
            // will be taken out then.
            tryMarkPrimaryDatanodeFailed();
            throw e;
          }
          lastPacket = Time.now();
          
          // update bytesSent
          long tmpBytesSent = one.getLastByteOffsetBlock();
          if (bytesSent < tmpBytesSent) {
            bytesSent = tmpBytesSent;
          }

          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            // wait for the close packet has been acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                dataQueue.wait(1000);// wait for acks to arrive from datanodes
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }

            endBlock();
          }
          if (progress != null) { progress.progress(); }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && dfsClient.clientRunning) {
            Thread.sleep(artificialSlowdown); 
          }
        } catch (Throwable e) {
          // Log warning if there was a real error.
          if (restartingNodeIndex == -1) {
            DFSClient.LOG.warn("DataStreamer Exception", e);
          }
          if (e instanceof IOException) {
            setLastException((IOException)e);
          }
          hasError = true;
          if (errorIndex == -1 && restartingNodeIndex == -1) {
            // Not a datanode issue
            streamerClosed = true;
          }
        }
      }
      closeInternal();
    }

    private void closeInternal() {
      closeResponder();       // close and join
      closeStream();
      streamerClosed = true;
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
    }

    /*
     * close both streamer and DFSOutputStream, should be called only 
     * by an external thread and only after all data to be sent has 
     * been flushed to datanode.
     * 
     * Interrupt this data streamer if force is true
     * 
     * @param force if this data stream is forced to be closed 
     */
    void close(boolean force) {
      streamerClosed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
      if (force) {
        this.interrupt();
      }
    }

    private void closeResponder() {
      if (response != null) {
        try {
          response.close();
          response.join();
        } catch (InterruptedException  e) {
          DFSClient.LOG.warn("Caught exception ", e);
        } finally {
          response = null;
        }
      }
    }

    private void closeStream() {
      if (blockStream != null) {
        try {
          blockStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockStream = null;
        }
      }
      if (blockReplyStream != null) {
        try {
          blockReplyStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockReplyStream = null;
        }
      }
      if (null != s) {
        try {
          s.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          s = null;
        }
      }
    }

    // The following synchronized methods are used whenever 
    // errorIndex or restartingNodeIndex is set. This is because
    // check & set needs to be atomic. Simply reading variables
    // does not require a synchronization. When responder is
    // not running (e.g. during pipeline recovery), there is no
    // need to use these methods.

    /** Set the error node index. Called by responder */
    synchronized void setErrorIndex(int idx) {
      errorIndex = idx;
    }

    /** Set the restarting node index. Called by responder */
    synchronized void setRestartingNodeIndex(int idx) {
      restartingNodeIndex = idx;
      // If the data streamer has already set the primary node
      // bad, clear it. It is likely that the write failed due to
      // the DN shutdown. Even if it was a real failure, the pipeline
      // recovery will take care of it.
      errorIndex = -1;      
    }

    /**
     * This method is used when no explicit error report was received,
     * but something failed. When the primary node is a suspect or
     * unsure about the cause, the primary node is marked as failed.
     */
    synchronized void tryMarkPrimaryDatanodeFailed() {
      // There should be no existing error and no ongoing restart.
      if ((errorIndex == -1) && (restartingNodeIndex == -1)) {
        errorIndex = 0;
      }
    }

    /**
     * Examine whether it is worth waiting for a node to restart.
     * @param index the node index
     */
    boolean shouldWaitForRestart(int index) {
      // Only one node in the pipeline.
      if (nodes.length == 1) {
        return true;
      }

      // Is it a local node?
      InetAddress addr = null;
      try {
        addr = InetAddress.getByName(nodes[index].getIpAddr());
      } catch (java.net.UnknownHostException e) {
        // we are passing an ip address. this should not happen.
        assert false;
      }

      if (addr != null && NetUtils.isLocalAddress(addr)) {
        return true;
      }
      return false;
    }

    //
    // Processes responses from the datanodes.  A packet is removed
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Daemon {

      private volatile boolean responderClosed = false;
      private DatanodeInfo[] targets = null;
      private boolean isLastPacketInBlock = false;

      ResponseProcessor (DatanodeInfo[] targets) {
        this.targets = targets;
      }

      @Override
      public void run() {

        setName("ResponseProcessor for block " + block);
        PipelineAck ack = new PipelineAck();

        while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
          // process responses from datanodes.
          try {
            // read an ack from the pipeline
            long begin = Time.monotonicNow();
            ack.readFields(blockReplyStream);
            long duration = Time.monotonicNow() - begin;
            if (duration > dfsclientSlowLogThresholdMs
                && ack.getSeqno() != Packet.HEART_BEAT_SEQNO) {
              DFSClient.LOG
                  .warn("Slow ReadProcessor read fields took " + duration
                      + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms); ack: "
                      + ack + ", targets: " + Arrays.asList(targets));
            } else if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("DFSClient " + ack);
            }

            long seqno = ack.getSeqno();
            // processes response status from datanodes.
            for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
              final Status reply = ack.getReply(i);
              // Restart will not be treated differently unless it is
              // the local node or the only one in the pipeline.
              if (PipelineAck.isRestartOOBStatus(reply) &&
                  shouldWaitForRestart(i)) {
                restartDeadline = dfsClient.getConf().datanodeRestartTimeout +
                    Time.now();
                setRestartingNodeIndex(i);
                String message = "A datanode is restarting: " + targets[i];
                DFSClient.LOG.info(message);
               throw new IOException(message);
              }
              // node error
              if (reply != SUCCESS) {
                setErrorIndex(i); // first bad datanode
                throw new IOException("Bad response " + reply +
                    " for block " + block +
                    " from datanode " + 
                    targets[i]);
              }
            }
            
            assert seqno != PipelineAck.UNKOWN_SEQNO : 
              "Ack for unknown seqno should be a failed ack: " + ack;
            if (seqno == Packet.HEART_BEAT_SEQNO) {  // a heartbeat ack
              continue;
            }

            // a success ack for a data packet
            Packet one;
            synchronized (dataQueue) {
              one = ackQueue.getFirst();
            }
            if (one.seqno != seqno) {
              throw new IOException("ResponseProcessor: Expecting seqno " +
                                    " for block " + block +
                                    one.seqno + " but received " + seqno);
            }
            isLastPacketInBlock = one.lastPacketInBlock;

            // Fail the packet write for testing in order to force a
            // pipeline recovery.
            if (DFSClientFaultInjector.get().failPacket() &&
                isLastPacketInBlock) {
              failPacket = true;
              throw new IOException(
                    "Failing the last packet for testing.");
            }
              
            // update bytesAcked
            block.setNumBytes(one.getLastByteOffsetBlock());

            synchronized (dataQueue) {
              lastAckedSeqno = seqno;
              ackQueue.removeFirst();
              dataQueue.notifyAll();
            }
          } catch (Exception e) {
            if (!responderClosed) {
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              hasError = true;
              // If no explicit error report was received, mark the primary
              // node as failed.
              tryMarkPrimaryDatanodeFailed();
              synchronized (dataQueue) {
                dataQueue.notifyAll();
              }
              if (restartingNodeIndex == -1) {
                DFSClient.LOG.warn("DFSOutputStream ResponseProcessor exception "
                     + " for block " + block, e);
              }
              responderClosed = true;
            }
          }
        }
      }

      void close() {
        responderClosed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError() throws IOException {
      if (response != null) {
        DFSClient.LOG.info("Error Recovery for " + block +
        " waiting for responder to exit. ");
        return true;
      }
      closeStream();

      // move packets from ack queue to front of the data queue
      synchronized (dataQueue) {
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      // Record the new pipeline failure recovery.
      if (lastAckedSeqnoBeforeFailure != lastAckedSeqno) {
         lastAckedSeqnoBeforeFailure = lastAckedSeqno;
         pipelineRecoveryCount = 1;
      } else {
        // If we had to recover the pipeline five times in a row for the
        // same packet, this client likely has corrupt data or corrupting
        // during transmission.
        if (++pipelineRecoveryCount > 5) {
          DFSClient.LOG.warn("Error recovering pipeline for writing " +
              block + ". Already retried 5 times for the same packet.");
          lastException.set(new IOException("Failing write. Tried pipeline " +
              "recovery 5 times without success."));
          streamerClosed = true;
          return false;
        }
      }
      boolean doSleep = setupPipelineForAppendOrRecovery();
      
      if (!streamerClosed && dfsClient.clientRunning) {
        if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

          // If we had an error while closing the pipeline, we go through a fast-path
          // where the BlockReceiver does not run. Instead, the DataNode just finalizes
          // the block immediately during the 'connect ack' process. So, we want to pull
          // the end-of-block packet from the dataQueue, since we don't actually have
          // a true pipeline to send it over.
          //
          // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
          // a client waiting on close() will be aware that the flush finished.
          synchronized (dataQueue) {
            Packet endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
            assert endOfBlockPacket.lastPacketInBlock;
            assert lastAckedSeqno == endOfBlockPacket.seqno - 1;
            lastAckedSeqno = endOfBlockPacket.seqno;
            dataQueue.notifyAll();
          }
          endBlock();
        } else {
          initDataStreaming();
        }
      }
      
      return doSleep;
    }

    private void setHflush() {
      isHflushed = true;
    }

    private int findNewDatanode(final DatanodeInfo[] original
        ) throws IOException {
      if (nodes.length != original.length + 1) {
        throw new IOException(
            new StringBuilder()
            .append("Failed to replace a bad datanode on the existing pipeline ")
            .append("due to no more good datanodes being available to try. ")
            .append("(Nodes: current=").append(Arrays.asList(nodes))
            .append(", original=").append(Arrays.asList(original)).append("). ")
            .append("The current failed datanode replacement policy is ")
            .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
            .append("a client may configure this via '")
            .append(DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY)
            .append("' in its configuration.")
            .toString());
      }
      for(int i = 0; i < nodes.length; i++) {
        int j = 0;
        for(; j < original.length && !nodes[i].equals(original[j]); j++);
        if (j == original.length) {
          return i;
        }
      }
      throw new IOException("Failed: new datanode not found: nodes="
          + Arrays.asList(nodes) + ", original=" + Arrays.asList(original));
    }

    private void addDatanode2ExistingPipeline() throws IOException {
      if (DataTransferProtocol.LOG.isDebugEnabled()) {
        DataTransferProtocol.LOG.debug("lastAckedSeqno = " + lastAckedSeqno);
      }
      /*
       * Is data transfer necessary?  We have the following cases.
       * 
       * Case 1: Failure in Pipeline Setup
       * - Append
       *    + Transfer the stored replica, which may be a RBW or a finalized.
       * - Create
       *    + If no data, then no transfer is required.
       *    + If there are data written, transfer RBW. This case may happens 
       *      when there are streaming failure earlier in this pipeline.
       *
       * Case 2: Failure in Streaming
       * - Append/Create:
       *    + transfer RBW
       * 
       * Case 3: Failure in Close
       * - Append/Create:
       *    + no transfer, let NameNode replicates the block.
       */
      if (!isAppend && lastAckedSeqno < 0
          && stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
        //no data have been written
        return;
      } else if (stage == BlockConstructionStage.PIPELINE_CLOSE
          || stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        //pipeline is closing
        return;
      }

      //get a new datanode
      final DatanodeInfo[] original = nodes;
      final LocatedBlock lb = dfsClient.namenode.getAdditionalDatanode(
          src, fileId, block, nodes, storageIDs,
          failed.toArray(new DatanodeInfo[failed.size()]),
          1, dfsClient.clientName);
      setPipeline(lb);

      //find the new datanode
      final int d = findNewDatanode(original);

      //transfer replica
      final DatanodeInfo src = d == 0? nodes[1]: nodes[d - 1];
      final DatanodeInfo[] targets = {nodes[d]};
      final StorageType[] targetStorageTypes = {storageTypes[d]};
      transfer(src, targets, targetStorageTypes, lb.getBlockToken());
    }

    private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
        final StorageType[] targetStorageTypes,
        final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //transfer replica to the new datanode
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock = createSocketForPipeline(src, 2, dfsClient);
        final long writeTimeout = dfsClient.getDatanodeWriteTimeout(2);
        
        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(sock);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(sock,
          unbufOut, unbufIn, dfsClient, blockToken, src);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        in = new DataInputStream(unbufIn);

        //send the TRANSFER_BLOCK request
        new Sender(out).transferBlock(block, blockToken, dfsClient.clientName,
            targets, targetStorageTypes);
        out.flush();

        //ack
        BlockOpResponseProto response =
          BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
        if (SUCCESS != response.getStatus()) {
          throw new IOException("Failed to add a datanode");
        }
      } finally {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }

    /**
     * Open a DataOutputStream to a DataNode pipeline so that 
     * it can be written to.
     * This happens when a file is appended or data streaming fails
     * It keeps on trying until a pipeline is setup
     */
    private boolean setupPipelineForAppendOrRecovery() throws IOException {
      // check number of datanodes
      if (nodes == null || nodes.length == 0) {
        String msg = "Could not get block locations. " + "Source file \""
            + src + "\" - Aborting...";
        DFSClient.LOG.warn(msg);
        setLastException(new IOException(msg));
        streamerClosed = true;
        return false;
      }
      
      boolean success = false;
      long newGS = 0L;
      while (!success && !streamerClosed && dfsClient.clientRunning) {
        // Sleep before reconnect if a dn is restarting.
        // This process will be repeated until the deadline or the datanode
        // starts back up.
        if (restartingNodeIndex >= 0) {
          // 4 seconds or the configured deadline period, whichever is shorter.
          // This is the retry interval and recovery will be retried in this
          // interval until timeout or success.
          long delay = Math.min(dfsClient.getConf().datanodeRestartTimeout,
              4000L);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            lastException.set(new IOException("Interrupted while waiting for " +
                "datanode to restart. " + nodes[restartingNodeIndex]));
            streamerClosed = true;
            return false;
          }
        }
        boolean isRecovery = hasError;
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex >= 0) {
          StringBuilder pipelineMsg = new StringBuilder();
          for (int j = 0; j < nodes.length; j++) {
            pipelineMsg.append(nodes[j]);
            if (j < nodes.length - 1) {
              pipelineMsg.append(", ");
            }
          }
          if (nodes.length <= 1) {
            lastException.set(new IOException("All datanodes " + pipelineMsg
                + " are bad. Aborting..."));
            streamerClosed = true;
            return false;
          }
          DFSClient.LOG.warn("Error Recovery for block " + block +
              " in pipeline " + pipelineMsg + 
              ": bad datanode " + nodes[errorIndex]);
          failed.add(nodes[errorIndex]);

          DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
          arraycopy(nodes, newnodes, errorIndex);

          final StorageType[] newStorageTypes = new StorageType[newnodes.length];
          arraycopy(storageTypes, newStorageTypes, errorIndex);

          final String[] newStorageIDs = new String[newnodes.length];
          arraycopy(storageIDs, newStorageIDs, errorIndex);
          
          setPipeline(newnodes, newStorageTypes, newStorageIDs);

          // Just took care of a node error while waiting for a node restart
          if (restartingNodeIndex >= 0) {
            // If the error came from a node further away than the restarting
            // node, the restart must have been complete.
            if (errorIndex > restartingNodeIndex) {
              restartingNodeIndex = -1;
            } else if (errorIndex < restartingNodeIndex) {
              // the node index has shifted.
              restartingNodeIndex--;
            } else {
              // this shouldn't happen...
              assert false;
            }
          }

          if (restartingNodeIndex == -1) {
            hasError = false;
          }
          lastException.set(null);
          errorIndex = -1;
        }

        // Check if replace-datanode policy is satisfied.
        if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(blockReplication,
            nodes, isAppend, isHflushed)) {
          addDatanode2ExistingPipeline();
        }

        // get a new generation stamp and an access token
        LocatedBlock lb = dfsClient.namenode.updateBlockForPipeline(block, dfsClient.clientName);
        newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getBlockToken();
        
        // set up the pipeline again with the remaining nodes
        if (failPacket) { // for testing
          success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
          failPacket = false;
          try {
            // Give DNs time to send in bad reports. In real situations,
            // good reports should follow bad ones, if client committed
            // with those nodes.
            Thread.sleep(2000);
          } catch (InterruptedException ie) {}
        } else {
          success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
        }

        if (restartingNodeIndex >= 0) {
          assert hasError == true;
          // check errorIndex set above
          if (errorIndex == restartingNodeIndex) {
            // ignore, if came from the restarting node
            errorIndex = -1;
          }
          // still within the deadline
          if (Time.now() < restartDeadline) {
            continue; // with in the deadline
          }
          // expired. declare the restarting node dead
          restartDeadline = 0;
          int expiredNodeIndex = restartingNodeIndex;
          restartingNodeIndex = -1;
          DFSClient.LOG.warn("Datanode did not restart in time: " +
              nodes[expiredNodeIndex]);
          // Mark the restarting node as failed. If there is any other failed
          // node during the last pipeline construction attempt, it will not be
          // overwritten/dropped. In this case, the restarting node will get
          // excluded in the following attempt, if it still does not come up.
          if (errorIndex == -1) {
            errorIndex = expiredNodeIndex;
          }
          // From this point on, normal pipeline recovery applies.
        }
      } // while

      if (success) {
        // update pipeline at the namenode
        ExtendedBlock newBlock = new ExtendedBlock(
            block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(), newGS);
        dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock,
            nodes, storageIDs);
        // update client side generation stamp
        block = newBlock;
      }
      return false; // do not sleep, continue processing
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     */
    private LocatedBlock nextBlockOutputStream() throws IOException {
      LocatedBlock lb = null;
      DatanodeInfo[] nodes = null;
      StorageType[] storageTypes = null;
      int count = dfsClient.getConf().nBlockWriteRetry;
      boolean success = false;
      ExtendedBlock oldBlock = block;
      do {
        hasError = false;
        lastException.set(null);
        errorIndex = -1;
        success = false;

        long startTime = Time.now();
        DatanodeInfo[] excluded =
            excludedNodes.getAllPresent(excludedNodes.asMap().keySet())
            .keySet()
            .toArray(new DatanodeInfo[0]);
        block = oldBlock;
        lb = locateFollowingBlock(startTime,
            excluded.length > 0 ? excluded : null);
        block = lb.getBlock();
        block.setNumBytes(0);
        bytesSent = 0;
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();
        storageTypes = lb.getStorageTypes();

        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, storageTypes, 0L, false);

        if (!success) {
          DFSClient.LOG.info("Abandoning " + block);
          dfsClient.namenode.abandonBlock(block, fileId, src,
              dfsClient.clientName);
          block = null;
          DFSClient.LOG.info("Excluding datanode " + nodes[errorIndex]);
          excludedNodes.put(nodes[errorIndex], nodes[errorIndex]);
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return lb;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    private boolean createBlockOutputStream(DatanodeInfo[] nodes,
        StorageType[] nodeStorageTypes, long newGS, boolean recoveryFlag) {
      if (nodes.length == 0) {
        DFSClient.LOG.info("nodes are empty for write pipeline of block "
            + block);
        return false;
      }
      Status pipelineStatus = SUCCESS;
      String firstBadLink = "";
      boolean checkRestart = false;
      if (DFSClient.LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          DFSClient.LOG.debug("pipeline = " + nodes[i]);
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks.set(true);

      int refetchEncryptionKey = 1;
      while (true) {
        boolean result = false;
        DataOutputStream out = null;
        try {
          assert null == s : "Previous socket unclosed";
          assert null == blockReplyStream : "Previous blockReplyStream unclosed";
          s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
          long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
          
          OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
          InputStream unbufIn = NetUtils.getInputStream(s);
          IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
          unbufOut = saslStreams.out;
          unbufIn = saslStreams.in;
          out = new DataOutputStream(new BufferedOutputStream(unbufOut,
              HdfsConstants.SMALL_BUFFER_SIZE));
          blockReplyStream = new DataInputStream(unbufIn);
  
          //
          // Xmit header info to datanode
          //
  
          BlockConstructionStage bcs = recoveryFlag? stage.getRecoveryStage(): stage;
          // send the request
          new Sender(out).writeBlock(block, nodeStorageTypes[0], accessToken,
              dfsClient.clientName, nodes, nodeStorageTypes, null, bcs, 
              nodes.length, block.getNumBytes(), bytesSent, newGS, checksum,
              cachingStrategy.get());
  
          // receive ack for connect
          BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
              PBHelper.vintPrefixed(blockReplyStream));
          pipelineStatus = resp.getStatus();
          firstBadLink = resp.getFirstBadLink();
          
          // Got an restart OOB ack.
          // If a node is already restarting, this status is not likely from
          // the same node. If it is from a different node, it is not
          // from the local datanode. Thus it is safe to treat this as a
          // regular node error.
          if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            restartingNodeIndex == -1) {
            checkRestart = true;
            throw new IOException("A datanode is restarting.");
          }
          if (pipelineStatus != SUCCESS) {
            if (pipelineStatus == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack with firstBadLink as "
                      + firstBadLink);
            } else {
              throw new IOException("Bad connect ack with firstBadLink as "
                  + firstBadLink);
            }
          }
          assert null == blockStream : "Previous blockStream unclosed";
          blockStream = out;
          result =  true; // success
          restartingNodeIndex = -1;
          hasError = false;
        } catch (IOException ie) {
          if (restartingNodeIndex == -1) {
            DFSClient.LOG.info("Exception in createBlockOutputStream", ie);
          }
          if (ie instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
            DFSClient.LOG.info("Will fetch a new encryption key and retry, " 
                + "encryption key was invalid when connecting to "
                + nodes[0] + " : " + ie);
            // The encryption key used is invalid.
            refetchEncryptionKey--;
            dfsClient.clearDataEncryptionKey();
            // Don't close the socket/exclude this node just yet. Try again with
            // a new encryption key.
            continue;
          }
  
          // find the datanode that matches
          if (firstBadLink.length() != 0) {
            for (int i = 0; i < nodes.length; i++) {
              // NB: Unconditionally using the xfer addr w/o hostname
              if (firstBadLink.equals(nodes[i].getXferAddr())) {
                errorIndex = i;
                break;
              }
            }
          } else {
            assert checkRestart == false;
            errorIndex = 0;
          }
          // Check whether there is a restart worth waiting for.
          if (checkRestart && shouldWaitForRestart(errorIndex)) {
            restartDeadline = dfsClient.getConf().datanodeRestartTimeout +
                Time.now();
            restartingNodeIndex = errorIndex;
            errorIndex = -1;
            DFSClient.LOG.info("Waiting for the datanode to be restarted: " +
                nodes[restartingNodeIndex]);
          }
          hasError = true;
          setLastException(ie);
          result =  false;  // error
        } finally {
          if (!result) {
            IOUtils.closeSocket(s);
            s = null;
            IOUtils.closeStream(out);
            out = null;
            IOUtils.closeStream(blockReplyStream);
            blockReplyStream = null;
          }
        }
        return result;
      }
    }

    private LocatedBlock locateFollowingBlock(long start,
        DatanodeInfo[] excludedNodes)  throws IOException {
      int retries = dfsClient.getConf().nBlockWriteLocateFollowingRetry;
      long sleeptime = 400;
      while (true) {
        long localstart = Time.now();
        while (true) {
          try {
            return dfsClient.namenode.addBlock(src, dfsClient.clientName,
                block, excludedNodes, fileId, favoredNodes);
          } catch (RemoteException e) {
            IOException ue = 
              e.unwrapRemoteException(FileNotFoundException.class,
                                      AccessControlException.class,
                                      NSQuotaExceededException.class,
                                      DSQuotaExceededException.class,
                                      UnresolvedPathException.class);
            if (ue != e) { 
              throw ue; // no need to retry these exceptions
            }
            
            
            if (NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {
              if (retries == 0) { 
                throw e;
              } else {
                --retries;
                DFSClient.LOG.info("Exception while adding a block", e);
                if (Time.now() - localstart > 5000) {
                  DFSClient.LOG.info("Waiting for replication for "
                      + (Time.now() - localstart) / 1000
                      + " seconds");
                }
                try {
                  DFSClient.LOG.warn("NotReplicatedYetException sleeping " + src
                      + " retries left " + retries);
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                  DFSClient.LOG.warn("Caught exception ", ie);
                }
              }
            } else {
              throw e;
            }

          }
        }
      } 
    }

    ExtendedBlock getBlock() {
      return block;
    }

    DatanodeInfo[] getNodes() {
      return nodes;
    }

    Token<BlockTokenIdentifier> getBlockToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      lastException.compareAndSet(null, e);
    }
  }

  /**
   * Create a socket for a write pipeline
   * @param first the first datanode 
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final String dnAddr = first.getXferAddr(
        client.getConf().connectToDnViaHostname);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), client.getConf().socketTimeout);
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if(DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }

  protected void checkClosed() throws IOException {
    if (closed) {
      IOException e = lastException.get();
      throw e != null ? e : new ClosedChannelException();
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  @VisibleForTesting
  public synchronized DatanodeInfo[] getPipeline() {
    if (streamer == null) {
      return null;
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return null;
    }
    DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
    for (int i = 0; i < currentNodes.length; i++) {
      value[i] = currentNodes[i];
    }
    return value;
  }

  private DFSOutputStream(DFSClient dfsClient, String src, Progressable progress,
      HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    super(checksum, checksum.getBytesPerChecksum(), checksum.getChecksumSize());
    this.dfsClient = dfsClient;
    this.src = src;
    this.fileId = stat.getFileId();
    this.blockSize = stat.getBlockSize();
    this.blockReplication = stat.getReplication();
    this.progress = progress;
    this.cachingStrategy = new AtomicReference<CachingStrategy>(
        dfsClient.getDefaultWriteCachingStrategy());
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug(
          "Set non-null progress callback on DFSOutputStream " + src);
    }
    
    final int bytesPerChecksum = checksum.getBytesPerChecksum();
    if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                            ") and blockSize(" + blockSize + 
                            ") do not match. " + "blockSize should be a " +
                            "multiple of io.bytes.per.checksum");
                            
    }
    this.checksum = checksum;
    this.dfsclientSlowLogThresholdMs =
      dfsClient.getConf().dfsclientSlowIoWarningThresholdMs;
  }

  /** Construct a new output stream for creating a file. */
  private DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
      EnumSet<CreateFlag> flag, Progressable progress,
      DataChecksum checksum, String[] favoredNodes) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

    computePacketChunkSize(dfsClient.getConf().writePacketSize,
        checksum.getBytesPerChecksum());

    streamer = new DataStreamer();
    if (favoredNodes != null && favoredNodes.length != 0) {
      streamer.setFavoredNodes(favoredNodes);
    }
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
      FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, Progressable progress, int buffersize,
      DataChecksum checksum, String[] favoredNodes) throws IOException {
    final HdfsFileStatus stat;
    try {
      stat = dfsClient.namenode.create(src, masked, dfsClient.clientName,
          new EnumSetWritable<CreateFlag>(flag), createParent, replication,
          blockSize);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     DSQuotaExceededException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
    final DFSOutputStream out = new DFSOutputStream(dfsClient, src, stat,
        flag, progress, checksum, favoredNodes);
    out.start();
    return out;
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
      FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, Progressable progress, int buffersize,
      DataChecksum checksum) throws IOException {
    return newStreamForCreate(dfsClient, src, masked, flag, createParent, replication,
        blockSize, progress, buffersize, checksum, null);
  }

  /** Construct a new output stream for append. */
  private DFSOutputStream(DFSClient dfsClient, String src,
      Progressable progress, LocatedBlock lastBlock, HdfsFileStatus stat,
      DataChecksum checksum) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    initialFileSize = stat.getLen(); // length of file when opened

    // The last partial block of the file has to be filled.
    if (lastBlock != null) {
      // indicate that we are appending to an existing block
      bytesCurBlock = lastBlock.getBlockSize();
      streamer = new DataStreamer(lastBlock, stat, checksum.getBytesPerChecksum());
    } else {
      computePacketChunkSize(dfsClient.getConf().writePacketSize,
          checksum.getBytesPerChecksum());
      streamer = new DataStreamer();
    }
  }

  static DFSOutputStream newStreamForAppend(DFSClient dfsClient, String src,
      int buffersize, Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    final DFSOutputStream out = new DFSOutputStream(dfsClient, src,
        progress, lastBlock, stat, checksum);
    out.start();
    return out;
  }

  private void computePacketChunkSize(int psize, int csize) {
    int chunkSize = csize + checksum.getChecksumSize();
    chunksPerPacket = Math.max(psize/chunkSize, 1);
    packetSize = chunkSize*chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
                ", chunkSize=" + chunkSize +
                ", chunksPerPacket=" + chunksPerPacket +
                ", packetSize=" + packetSize);
    }
  }

  private void queueCurrentPacket() {
    synchronized (dataQueue) {
      if (currentPacket == null) return;
      dataQueue.addLast(currentPacket);
      lastQueuedSeqno = currentPacket.seqno;
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Queued packet " + currentPacket.seqno);
      }
      currentPacket = null;
      dataQueue.notifyAll();
    }
  }

  private void waitAndQueueCurrentPacket() throws IOException {
    synchronized (dataQueue) {
      try {
      // If queue is full, then wait till we have enough space
      while (!closed && dataQueue.size() + ackQueue.size()  > MAX_PACKETS) {
        try {
          dataQueue.wait();
        } catch (InterruptedException e) {
          // If we get interrupted while waiting to queue data, we still need to get rid
          // of the current packet. This is because we have an invariant that if
          // currentPacket gets full, it will get queued before the next writeChunk.
          //
          // Rather than wait around for space in the queue, we should instead try to
          // return to the caller as soon as possible, even though we slightly overrun
          // the MAX_PACKETS length.
          Thread.currentThread().interrupt();
          break;
        }
      }
      checkClosed();
      queueCurrentPacket();
      } catch (ClosedChannelException e) {
      }
    }
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum) 
                                                        throws IOException {
    dfsClient.checkOpen();
    checkClosed();

    int cklen = checksum.length;
    int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
                            " is larger than supported  bytesPerChecksum " +
                            bytesPerChecksum);
    }
    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
                            this.checksum.getChecksumSize() + 
                            " but found to be " + checksum.length);
    }

    if (currentPacket == null) {
      currentPacket = new Packet(packetSize, chunksPerPacket, 
          bytesCurBlock, currentSeqno++, this.checksum.getChecksumSize());
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
            currentPacket.seqno +
            ", src=" + src +
            ", packetSize=" + packetSize +
            ", chunksPerPacket=" + chunksPerPacket +
            ", bytesCurBlock=" + bytesCurBlock);
      }
    }

    currentPacket.writeChecksum(checksum, 0, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.numChunks++;
    bytesCurBlock += len;

    // If packet is full, enqueue it for transmission
    //
    if (currentPacket.numChunks == currentPacket.maxChunks ||
        bytesCurBlock == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
            currentPacket.seqno +
            ", src=" + src +
            ", bytesCurBlock=" + bytesCurBlock +
            ", blockSize=" + blockSize +
            ", appendChunk=" + appendChunk);
      }
      waitAndQueueCurrentPacket();

      // If the reopened file did not end at chunk boundary and the above
      // write filled up its partial chunk. Tell the summer to generate full 
      // crc chunks from now on.
      if (appendChunk && bytesCurBlock%bytesPerChecksum == 0) {
        appendChunk = false;
        resetChecksumChunk(bytesPerChecksum);
      }

      if (!appendChunk) {
        int psize = Math.min((int)(blockSize-bytesCurBlock), dfsClient.getConf().writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
      }
      //
      // if encountering a block boundary, send an empty packet to 
      // indicate the end of block and reset bytesCurBlock.
      //
      if (bytesCurBlock == blockSize) {
        currentPacket = new Packet(0, 0, bytesCurBlock, 
            currentSeqno++, this.checksum.getChecksumSize());
        currentPacket.lastPacketInBlock = true;
        currentPacket.syncBlock = shouldSyncBlock;
        waitAndQueueCurrentPacket();
        bytesCurBlock = 0;
        lastFlushOffset = 0;
      }
    }
  }
  
  /**
   * Flushes out to all replicas of the block. The data is in the buffers
   * of the DNs but not necessarily in the DN's OS buffers.
   *
   * It is a synchronous operation. When it returns,
   * it guarantees that flushed data become visible to new readers. 
   * It is not guaranteed that data has been flushed to 
   * persistent store on the datanode. 
   * Block allocations are persisted on namenode.
   */
  @Override
  public void hflush() throws IOException {
    flushOrSync(false, EnumSet.noneOf(SyncFlag.class));
  }

  @Override
  public void hsync() throws IOException {
    hsync(EnumSet.noneOf(SyncFlag.class));
  }
  
  /**
   * The expected semantics is all data have flushed out to all replicas 
   * and all replicas have done posix fsync equivalent - ie the OS has 
   * flushed it to the disk device (but the disk may have it in its cache).
   * 
   * Note that only the current block is flushed to the disk device.
   * To guarantee durable sync across block boundaries the stream should
   * be created with {@link CreateFlag#SYNC_BLOCK}.
   * 
   * @param syncFlags
   *          Indicate the semantic of the sync. Currently used to specify
   *          whether or not to update the block length in NameNode.
   */
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    flushOrSync(true, syncFlags);
  }

  /**
   * Flush/Sync buffered data to DataNodes.
   * 
   * @param isSync
   *          Whether or not to require all replicas to flush data to the disk
   *          device
   * @param syncFlags
   *          Indicate extra detailed semantic of the flush/sync. Currently
   *          mainly used to specify whether or not to update the file length in
   *          the NameNode
   * @throws IOException
   */
  private void flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags)
      throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    try {
      long toWaitFor;
      long lastBlockLength = -1L;
      boolean updateLength = syncFlags.contains(SyncFlag.UPDATE_LENGTH);
      synchronized (this) {
        /* Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed.
         * After the flush, reset the bytesCurBlock back to its previous value,
         * any partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;
        Packet oldCurrentPacket = currentPacket;
        // flush checksum buffer, but keep checksum buffer intact
        flushBuffer(true);
        // bytesCurBlock potentially incremented if there was buffered data

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug(
            "DFSClient flush() : saveOffset " + saveOffset +  
            " bytesCurBlock " + bytesCurBlock +
            " lastFlushOffset " + lastFlushOffset);
        }
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {
          assert bytesCurBlock > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;
          if (isSync && currentPacket == null) {
            // Nothing to send right now,
            // but sync was requested.
            // Send an empty packet
            currentPacket = new Packet(packetSize, chunksPerPacket,
                bytesCurBlock, currentSeqno++, this.checksum.getChecksumSize());
          }
        } else {
          // We already flushed up to this offset.
          // This means that we haven't written anything since the last flush
          // (or the beginning of the file). Hence, we should not have any
          // packet queued prior to this call, since the last flush set
          // currentPacket = null.
          assert oldCurrentPacket == null :
            "Empty flush should not occur with a currentPacket";

          if (isSync && bytesCurBlock > 0) {
            // Nothing to send right now,
            // and the block was partially written,
            // and sync was requested.
            // So send an empty sync packet.
            currentPacket = new Packet(packetSize, chunksPerPacket,
                bytesCurBlock, currentSeqno++, this.checksum.getChecksumSize());
          } else {
            // just discard the current packet since it is already been sent.
            currentPacket = null;
          }
        }
        if (currentPacket != null) {
          currentPacket.syncBlock = isSync;
          waitAndQueueCurrentPacket();          
        }
        // Restore state of stream. Record the last flush offset 
        // of the last full chunk that was flushed.
        //
        bytesCurBlock = saveOffset;
        toWaitFor = lastQueuedSeqno;
      } // end synchronized

      waitForAckedSeqno(toWaitFor);

      // update the block length first time irrespective of flag
      if (updateLength || persistBlocks.get()) {
        synchronized (this) {
          if (streamer != null && streamer.block != null) {
            lastBlockLength = streamer.block.getNumBytes();
          }
        }
      }
      // If 1) any new blocks were allocated since the last flush, or 2) to
      // update length in NN is required, then persist block locations on
      // namenode.
      if (persistBlocks.getAndSet(false) || updateLength) {
        try {
          dfsClient.namenode.fsync(src, fileId,
              dfsClient.clientName, lastBlockLength);
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Unable to persist blocks in hflush for " + src, ioe);
          // If we got an error here, it might be because some other thread called
          // close before our hflush completed. In that case, we should throw an
          // exception that the stream is closed.
          checkClosed();
          // If we aren't closed but failed to sync, we should expose that to the
          // caller.
          throw ioe;
        }
      }

      synchronized(this) {
        if (streamer != null) {
          streamer.setHflush();
        }
      }
    } catch (InterruptedIOException interrupt) {
      // This kind of error doesn't mean that the stream itself is broken - just the
      // flushing thread got interrupted. So, we shouldn't close down the writer,
      // but instead just propagate the error
      throw interrupt;
    } catch (IOException e) {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!closed) {
          lastException.set(new IOException("IOException flush:" + e));
          closeThreads(true);
        }
      }
      throw e;
    }
  }

  /**
   * @deprecated use {@link HdfsDataOutputStream#getCurrentBlockReplication()}.
   */
  @Deprecated
  public synchronized int getNumCurrentReplicas() throws IOException {
    return getCurrentBlockReplication();
  }

  /**
   * Note that this is not a public API;
   * use {@link HdfsDataOutputStream#getCurrentBlockReplication()} instead.
   * 
   * @return the number of valid replicas of the current block
   */
  public synchronized int getCurrentBlockReplication() throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    if (streamer == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    return currentNodes.length;
  }
  
  /**
   * Waits till all existing data is flushed and confirmations 
   * received from datanodes. 
   */
  private void flushInternal() throws IOException {
    long toWaitFor;
    synchronized (this) {
      dfsClient.checkOpen();
      checkClosed();
      //
      // If there is data in the current buffer, send it across
      //
      queueCurrentPacket();
      toWaitFor = lastQueuedSeqno;
    }

    waitForAckedSeqno(toWaitFor);
  }

  private void waitForAckedSeqno(long seqno) throws IOException {
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Waiting for ack for: " + seqno);
    }
    long begin = Time.monotonicNow();
    try {
      synchronized (dataQueue) {
        while (!closed) {
          checkClosed();
          if (lastAckedSeqno >= seqno) {
            break;
          }
          try {
            dataQueue.wait(1000); // when we receive an ack, we notify on
                                  // dataQueue
          } catch (InterruptedException ie) {
            throw new InterruptedIOException(
                "Interrupted while waiting for data to be acknowledged by pipeline");
          }
        }
      }
      checkClosed();
    } catch (ClosedChannelException e) {
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > dfsclientSlowLogThresholdMs) {
      DFSClient.LOG.warn("Slow waitForAckedSeqno took " + duration
          + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms)");
    }
  }

  private synchronized void start() {
    streamer.start();
  }
  
  /**
   * Aborts this output stream and releases any system 
   * resources associated with this stream.
   */
  synchronized void abort() throws IOException {
    if (closed) {
      return;
    }
    streamer.setLastException(new IOException("Lease timeout of "
        + (dfsClient.getHdfsTimeout()/1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  private void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      if (s != null) {
        s.close();
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer = null;
      s = null;
      closed = true;
    }
  }
  
  /**
   * Closes this output stream and releases any system 
   * resources associated with this stream.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      IOException e = lastException.getAndSet(null);
      if (e == null)
        return;
      else
        throw e;
    }

    try {
      flushBuffer();       // flush from all upper layers

      if (currentPacket != null) { 
        waitAndQueueCurrentPacket();
      }

      if (bytesCurBlock != 0) {
        // send an empty packet to mark the end of the block
        currentPacket = new Packet(0, 0, bytesCurBlock, 
            currentSeqno++, this.checksum.getChecksumSize());
        currentPacket.lastPacketInBlock = true;
        currentPacket.syncBlock = shouldSyncBlock;
      }

      flushInternal();             // flush all data to Datanodes
      // get last block before destroying the streamer
      ExtendedBlock lastBlock = streamer.getBlock();
      closeThreads(false);
      completeFile(lastBlock);
      dfsClient.endFileLease(fileId);
    } catch (ClosedChannelException e) {
    } finally {
      closed = true;
    }
  }

  // should be called holding (this) lock since setTestFilename() may 
  // be called during unit tests
  private void completeFile(ExtendedBlock last) throws IOException {
    long localstart = Time.now();
    long localTimeout = 400;
    boolean fileComplete = false;
    int retries = dfsClient.getConf().nBlockWriteLocateFollowingRetry;
    while (!fileComplete) {
      fileComplete =
          dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId);
      if (!fileComplete) {
        final int hdfsTimeout = dfsClient.getHdfsTimeout();
        if (!dfsClient.clientRunning ||
              (hdfsTimeout > 0 && localstart + hdfsTimeout < Time.now())) {
            String msg = "Unable to close file because dfsclient " +
                          " was unable to contact the HDFS servers." +
                          " clientRunning " + dfsClient.clientRunning +
                          " hdfsTimeout " + hdfsTimeout;
            DFSClient.LOG.info(msg);
            throw new IOException(msg);
        }
        try {
          if (retries == 0) {
            throw new IOException("Unable to close file because the last block"
                + " does not have enough number of replicas.");
          }
          retries--;
          Thread.sleep(localTimeout);
          localTimeout *= 2;
          if (Time.now() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.warn("Caught exception ", ie);
        }
      }
    }
  }

  @VisibleForTesting
  public void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  @VisibleForTesting
  public synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = (checksum.getBytesPerChecksum() + 
                  checksum.getChecksumSize()) * chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  long getInitialLen() {
    return initialFileSize;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  synchronized Token<BlockTokenIdentifier> getBlockToken() {
    return streamer.getBlockToken();
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    CachingStrategy prevStrategy, nextStrategy;
    // CachingStrategy is immutable.  So build a new CachingStrategy with the
    // modifications we want, and compare-and-swap it in.
    do {
      prevStrategy = this.cachingStrategy.get();
      nextStrategy = new CachingStrategy.Builder(prevStrategy).
                        setDropBehind(dropBehind).build();
    } while (!this.cachingStrategy.compareAndSet(prevStrategy, nextStrategy));
  }

  @VisibleForTesting
  ExtendedBlock getBlock() {
    return streamer.getBlock();
  }

  @VisibleForTesting
  public long getFileId() {
    return fileId;
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }
}
