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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

/**
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 * {@link BlockReaderLocal} works as follows:
 * <ul>
 * <li>The client performing short circuit reads must be configured at the
 * datanode.</li>
 * <li>The client gets the path to the file where block is stored using
 * {@link org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol#getBlockLocalPathInfo(ExtendedBlock, Token)}
 * RPC call</li>
 * <li>Client uses kerberos authentication to connect to the datanode over RPC,
 * if security is enabled.</li>
 * </ul>
 */
class BlockReaderLocal extends RemoteBlockReader2 {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  //Stores the cache and proxy for a local datanode.
  private static class LocalDatanodeInfo {
    private ClientDatanodeProtocol proxy = null;
    private final Map<ExtendedBlock, BlockLocalPathInfo> cache;

    LocalDatanodeInfo() {
      final int cacheSize = 10000;
      final float hashTableLoadFactor = 0.75f;
      int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
      cache = Collections
          .synchronizedMap(new LinkedHashMap<ExtendedBlock, BlockLocalPathInfo>(
              hashTableCapacity, hashTableLoadFactor, true) {
            private static final long serialVersionUID = 1;

            @Override
            protected boolean removeEldestEntry(
                Map.Entry<ExtendedBlock, BlockLocalPathInfo> eldest) {
              return size() > cacheSize;
            }
          });
    }

    private synchronized ClientDatanodeProtocol getDatanodeProxy(
        DatanodeInfo node, Configuration conf, int socketTimeout)
        throws IOException {
      if (proxy == null) {
        proxy = DFSUtil.createClientDatanodeProtocolProxy(node, conf,
            socketTimeout);
      }
      return proxy;
    }
    
    private synchronized void resetDatanodeProxy() {
      if (null != proxy) {
        RPC.stopProxy(proxy);
        proxy = null;
      }
    }

    private BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b) {
      return cache.get(b);
    }

    private void setBlockLocalPathInfo(ExtendedBlock b, BlockLocalPathInfo info) {
      cache.put(b, info);
    }

    private void removeBlockLocalPathInfo(ExtendedBlock b) {
      cache.remove(b);
    }
  }
  
  // Multiple datanodes could be running on the local machine. Store proxies in
  // a map keyed by the ipc port of the datanode.
  private static Map<Integer, LocalDatanodeInfo> localDatanodeInfoMap = new HashMap<Integer, LocalDatanodeInfo>();

  private final FileInputStream dataIn; // reader for the data file

  private FileInputStream checksumIn;   // reader for the checksum file

  private int offsetFromChunkBoundary;
  
  ByteBuffer dataBuff = null;
  ByteBuffer checksumBuff = null;
  
  /**
   * The only way this object can be instantiated.
   */
  static BlockReaderLocal newBlockReader(Configuration conf, String file,
      ExtendedBlock blk, Token<BlockTokenIdentifier> token, DatanodeInfo node,
      int socketTimeout, long startOffset, long length) throws IOException {

    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node
        .getIpcPort());
    // check the cache first
    BlockLocalPathInfo pathinfo = localDatanodeInfo.getBlockLocalPathInfo(blk);
    if (pathinfo == null) {
      pathinfo = getBlockPathInfo(blk, node, conf, socketTimeout, token);
    }

    // check to see if the file exists. It may so happen that the
    // HDFS file has been deleted and this block-lookup is occurring
    // on behalf of a new HDFS file. This time, the block file could
    // be residing in a different portion of the fs.data.dir directory.
    // In this case, we remove this entry from the cache. The next
    // call to this method will re-populate the cache.
    FileInputStream dataIn = null;
    FileInputStream checksumIn = null;
    BlockReaderLocal localBlockReader = null;
    boolean skipChecksumCheck = skipChecksumCheck(conf);
    try {
      // get a local file system
      File blkfile = new File(pathinfo.getBlockPath());
      dataIn = new FileInputStream(blkfile);

      if (LOG.isDebugEnabled()) {
        LOG.debug("New BlockReaderLocal for file " + blkfile + " of size "
            + blkfile.length() + " startOffset " + startOffset + " length "
            + length + " short circuit checksum " + skipChecksumCheck);
      }

      if (!skipChecksumCheck) {
        // get the metadata file
        File metafile = new File(pathinfo.getMetaPath());
        checksumIn = new FileInputStream(metafile);

        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader
            .readHeader(new DataInputStream(checksumIn));
        short version = header.getVersion();
        if (version != FSDataset.METADATA_VERSION) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + blk + " ignoring ...");
        }
        DataChecksum checksum = header.getChecksum();
        long firstChunkOffset = startOffset
            - (startOffset % checksum.getBytesPerChecksum());
        localBlockReader = new BlockReaderLocal(conf, file, blk, token,
            startOffset, length, pathinfo, checksum, true, dataIn,
            firstChunkOffset, checksumIn);
      } else {
        localBlockReader = new BlockReaderLocal(conf, file, blk, token,
            startOffset, length, pathinfo, dataIn);
      }
    } catch (IOException e) {
      // remove from cache
      localDatanodeInfo.removeBlockLocalPathInfo(blk);
      DFSClient.LOG.warn("BlockReaderLocal: Removing " + blk
          + " from cache because local file " + pathinfo.getBlockPath()
          + " could not be opened.");
      throw e;
    } finally {
      if (localBlockReader == null) {
        if (dataIn != null) {
          dataIn.close();
        }
        if (checksumIn != null) {
          checksumIn.close();
        }
      }
    }
    return localBlockReader;
  }
  
  private static synchronized LocalDatanodeInfo getLocalDatanodeInfo(int port) {
    LocalDatanodeInfo ldInfo = localDatanodeInfoMap.get(port);
    if (ldInfo == null) {
      ldInfo = new LocalDatanodeInfo();
      localDatanodeInfoMap.put(port, ldInfo);
    }
    return ldInfo;
  }
  
  private static BlockLocalPathInfo getBlockPathInfo(ExtendedBlock blk,
      DatanodeInfo node, Configuration conf, int timeout,
      Token<BlockTokenIdentifier> token) throws IOException {
    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node.ipcPort);
    BlockLocalPathInfo pathinfo = null;
    ClientDatanodeProtocol proxy = localDatanodeInfo.getDatanodeProxy(node,
        conf, timeout);
    try {
      // make RPC to local datanode to find local pathnames of blocks
      pathinfo = proxy.getBlockLocalPathInfo(blk, token);
      if (pathinfo != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cached location of block " + blk + " as " + pathinfo);
        }
        localDatanodeInfo.setBlockLocalPathInfo(blk, pathinfo);
      }
    } catch (IOException e) {
      localDatanodeInfo.resetDatanodeProxy(); // Reset proxy on error
      throw e;
    }
    return pathinfo;
  }
  
  private static boolean skipChecksumCheck(Configuration conf) {
    return conf.getBoolean(
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT);
  }
  
  private BlockReaderLocal(Configuration conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, FileInputStream dataIn)
      throws IOException {
    this(conf, hdfsfile, block, token, startOffset, length, pathinfo,
        DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL, 4), false,
        dataIn, startOffset, null);
  }

  private BlockReaderLocal(Configuration conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, DataChecksum checksum,
      boolean verifyChecksum, FileInputStream dataIn, long firstChunkOffset,
      FileInputStream checksumIn) throws IOException {
    super(hdfsfile, block.getBlockPoolId(), block.getBlockId(), dataIn
        .getChannel(), checksum, verifyChecksum, startOffset, firstChunkOffset,
        length, null);
    this.dataIn = dataIn;
    this.checksumIn = checksumIn;
    this.offsetFromChunkBoundary = (int) (startOffset-firstChunkOffset);
    dataBuff = bufferPool.getBuffer(bytesPerChecksum*64);
    checksumBuff = bufferPool.getBuffer(checksumSize*64);
    //Initially the buffers have nothing to read.
    dataBuff.flip();
    checksumBuff.flip();
    long toSkip = firstChunkOffset;
    while (toSkip > 0) {
      long skipped = dataIn.skip(toSkip);
      if (skipped == 0) {
        throw new IOException("Couldn't initialize input stream");
      }
      toSkip -= skipped;
    }
    if (checksumIn != null) {
      long checkSumOffset = (firstChunkOffset / bytesPerChecksum)
          * checksumSize;
      while (checkSumOffset > 0) {
        long skipped = checksumIn.skip(checkSumOffset);
        if (skipped == 0) {
          throw new IOException("Couldn't initialize checksum input stream");
        }
        checkSumOffset -= skipped;
      }
    }
  }

  private int readIntoBuffer(FileInputStream stream, ByteBuffer buf)
      throws IOException {
    int bytesRead = stream.getChannel().read(buf);
    if (bytesRead < 0) {
      //EOF
      return bytesRead;
    }
    while (buf.remaining() > 0) {
      int n = stream.getChannel().read(buf);
      if (n < 0) {
        //EOF
        return bytesRead;
      }
      bytesRead += n;
    }
    return bytesRead;
  }
  
  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.info("read off " + off + " len " + len);
    }
    if (!verifyChecksum) {
      return dataIn.read(buf, off, len);
    } else {
      int dataRead = -1;
      if (dataBuff.remaining() == 0) {
        dataBuff.clear();
        checksumBuff.clear();
        dataRead = readIntoBuffer(dataIn, dataBuff);
        readIntoBuffer(checksumIn, checksumBuff);
        checksumBuff.flip();
        dataBuff.flip();
        if (verifyChecksum) {
          checksum.verifyChunkedSums(dataBuff, checksumBuff, filename,
              this.startOffset);
        }
      } else {
        dataRead = dataBuff.remaining();
      }
      if (dataRead > 0) {
        int nRead = Math.min(dataRead - offsetFromChunkBoundary, len);
        if (offsetFromChunkBoundary > 0) {
          dataBuff.position(offsetFromChunkBoundary);
          // Its either end of file or dataRead is greater than the
          // offsetFromChunkBoundary
          offsetFromChunkBoundary = 0;
        }
        if (nRead > 0) {
          dataBuff.get(buf, off, nRead);
          return nRead;
        } else {
          return 0;
        }
      } else {
        return -1;
      }
    }
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("skip " + n);
    }
    if (!verifyChecksum) {
      return dataIn.skip(n);
    } else {
     return super.skip(n);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    dataIn.close();
    if (checksumIn != null) {
      checksumIn.close();
    }
    if (dataBuff != null) {
      bufferPool.returnBuffer(dataBuff);
      dataBuff = null;
    }
    if (checksumBuff != null) {
      bufferPool.returnBuffer(checksumBuff);
      checksumBuff = null;
    }
    super.close();
  }
}