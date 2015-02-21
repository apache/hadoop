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
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * BlockReaderLocalLegacy enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 *
 * This is the legacy implementation based on HDFS-2246, which requires
 * permissions on the datanode to be set so that clients can directly access the
 * blocks. The new implementation based on HDFS-347 should be preferred on UNIX
 * systems where the required native code has been implemented.<br>
 *
 * {@link BlockReaderLocalLegacy} works as follows:
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
class BlockReaderLocalLegacy implements BlockReader {
  private static final Log LOG = LogFactory.getLog(BlockReaderLocalLegacy.class);

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
        UserGroupInformation ugi, final DatanodeInfo node,
        final Configuration conf, final int socketTimeout,
        final boolean connectToDnViaHostname) throws IOException {
      if (proxy == null) {
        try {
          proxy = ugi.doAs(new PrivilegedExceptionAction<ClientDatanodeProtocol>() {
            @Override
            public ClientDatanodeProtocol run() throws Exception {
              return DFSUtil.createClientDatanodeProtocolProxy(node, conf,
                  socketTimeout, connectToDnViaHostname);
            }
          });
        } catch (InterruptedException e) {
          LOG.warn("encountered exception ", e);
        }
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
  private static final Map<Integer, LocalDatanodeInfo> localDatanodeInfoMap = new HashMap<Integer, LocalDatanodeInfo>();

  private final FileInputStream dataIn; // reader for the data file
  private final FileInputStream checksumIn;   // reader for the checksum file

  /**
   * Offset from the most recent chunk boundary at which the next read should
   * take place. Is only set to non-zero at construction time, and is
   * decremented (usually to 0) by subsequent reads. This avoids having to do a
   * checksum read at construction to position the read cursor correctly.
   */
  private int offsetFromChunkBoundary;
  
  private byte[] skipBuf = null;

  /**
   * Used for checksummed reads that need to be staged before copying to their
   * output buffer because they are either a) smaller than the checksum chunk
   * size or b) issued by the slower read(byte[]...) path
   */
  private ByteBuffer slowReadBuff = null;
  private ByteBuffer checksumBuff = null;
  private DataChecksum checksum;
  private final boolean verifyChecksum;

  private static final DirectBufferPool bufferPool = new DirectBufferPool();

  private final int bytesPerChecksum;
  private final int checksumSize;

  /** offset in block where reader wants to actually read */
  private long startOffset;
  private final String filename;
  private long blockId;
  
  /**
   * The only way this object can be instantiated.
   */
  static BlockReaderLocalLegacy newBlockReader(DFSClient.Conf conf,
      UserGroupInformation userGroupInformation,
      Configuration configuration, String file, ExtendedBlock blk,
      Token<BlockTokenIdentifier> token, DatanodeInfo node, 
      long startOffset, long length, StorageType storageType)
      throws IOException {
    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node
        .getIpcPort());
    // check the cache first
    BlockLocalPathInfo pathinfo = localDatanodeInfo.getBlockLocalPathInfo(blk);
    if (pathinfo == null) {
      if (userGroupInformation == null) {
        userGroupInformation = UserGroupInformation.getCurrentUser();
      }
      pathinfo = getBlockPathInfo(userGroupInformation, blk, node,
          configuration, conf.socketTimeout, token,
          conf.connectToDnViaHostname, storageType);
    }

    // check to see if the file exists. It may so happen that the
    // HDFS file has been deleted and this block-lookup is occurring
    // on behalf of a new HDFS file. This time, the block file could
    // be residing in a different portion of the fs.data.dir directory.
    // In this case, we remove this entry from the cache. The next
    // call to this method will re-populate the cache.
    FileInputStream dataIn = null;
    FileInputStream checksumIn = null;
    BlockReaderLocalLegacy localBlockReader = null;
    boolean skipChecksumCheck = conf.skipShortCircuitChecksums ||
        storageType.isTransient();
    try {
      // get a local file system
      File blkfile = new File(pathinfo.getBlockPath());
      dataIn = new FileInputStream(blkfile);

      if (LOG.isDebugEnabled()) {
        LOG.debug("New BlockReaderLocalLegacy for file " + blkfile + " of size "
            + blkfile.length() + " startOffset " + startOffset + " length "
            + length + " short circuit checksum " + !skipChecksumCheck);
      }

      if (!skipChecksumCheck) {
        // get the metadata file
        File metafile = new File(pathinfo.getMetaPath());
        checksumIn = new FileInputStream(metafile);

        final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(
            new DataInputStream(checksumIn), blk);
        long firstChunkOffset = startOffset
            - (startOffset % checksum.getBytesPerChecksum());
        localBlockReader = new BlockReaderLocalLegacy(conf, file, blk, token,
            startOffset, length, pathinfo, checksum, true, dataIn,
            firstChunkOffset, checksumIn);
      } else {
        localBlockReader = new BlockReaderLocalLegacy(conf, file, blk, token,
            startOffset, length, pathinfo, dataIn);
      }
    } catch (IOException e) {
      // remove from cache
      localDatanodeInfo.removeBlockLocalPathInfo(blk);
      DFSClient.LOG.warn("BlockReaderLocalLegacy: Removing " + blk
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
  
  private static BlockLocalPathInfo getBlockPathInfo(UserGroupInformation ugi,
      ExtendedBlock blk, DatanodeInfo node, Configuration conf, int timeout,
      Token<BlockTokenIdentifier> token, boolean connectToDnViaHostname,
      StorageType storageType) throws IOException {
    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node.getIpcPort());
    BlockLocalPathInfo pathinfo = null;
    ClientDatanodeProtocol proxy = localDatanodeInfo.getDatanodeProxy(ugi, node,
        conf, timeout, connectToDnViaHostname);
    try {
      // make RPC to local datanode to find local pathnames of blocks
      pathinfo = proxy.getBlockLocalPathInfo(blk, token);
      // We cannot cache the path information for a replica on transient storage.
      // If the replica gets evicted, then it moves to a different path.  Then,
      // our next attempt to read from the cached path would fail to find the
      // file.  Additionally, the failure would cause us to disable legacy
      // short-circuit read for all subsequent use in the ClientContext.  Unlike
      // the newer short-circuit read implementation, we have no communication
      // channel for the DataNode to notify the client that the path has been
      // invalidated.  Therefore, our only option is to skip caching.
      if (pathinfo != null && !storageType.isTransient()) {
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
  
  private static int getSlowReadBufferNumChunks(int bufferSizeBytes,
      int bytesPerChecksum) {
    if (bufferSizeBytes < bytesPerChecksum) {
      throw new IllegalArgumentException("Configured BlockReaderLocalLegacy " +
          "buffer size (" + bufferSizeBytes + ") is not large enough to hold " +
          "a single chunk (" + bytesPerChecksum +  "). Please configure " +
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY +
          " appropriately");
    }

    // Round down to nearest chunk size
    return bufferSizeBytes / bytesPerChecksum;
  }

  private BlockReaderLocalLegacy(DFSClient.Conf conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, FileInputStream dataIn)
      throws IOException {
    this(conf, hdfsfile, block, token, startOffset, length, pathinfo,
        DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 4), false,
        dataIn, startOffset, null);
  }

  private BlockReaderLocalLegacy(DFSClient.Conf conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, DataChecksum checksum,
      boolean verifyChecksum, FileInputStream dataIn, long firstChunkOffset,
      FileInputStream checksumIn) throws IOException {
    this.filename = hdfsfile;
    this.checksum = checksum;
    this.verifyChecksum = verifyChecksum;
    this.startOffset = Math.max(startOffset, 0);
    this.blockId = block.getBlockId();

    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();

    this.dataIn = dataIn;
    this.checksumIn = checksumIn;
    this.offsetFromChunkBoundary = (int) (startOffset-firstChunkOffset);

    int chunksPerChecksumRead = getSlowReadBufferNumChunks(
        conf.shortCircuitBufferSize, bytesPerChecksum);
    slowReadBuff = bufferPool.getBuffer(bytesPerChecksum * chunksPerChecksumRead);
    checksumBuff = bufferPool.getBuffer(checksumSize * chunksPerChecksumRead);
    // Initially the buffers have nothing to read.
    slowReadBuff.flip();
    checksumBuff.flip();
    boolean success = false;
    try {
      // Skip both input streams to beginning of the chunk containing startOffset
      IOUtils.skipFully(dataIn, firstChunkOffset);
      if (checksumIn != null) {
        long checkSumOffset = (firstChunkOffset / bytesPerChecksum) * checksumSize;
        IOUtils.skipFully(checksumIn, checkSumOffset);
      }
      success = true;
    } finally {
      if (!success) {
        bufferPool.returnBuffer(slowReadBuff);
        bufferPool.returnBuffer(checksumBuff);
      }
    }
  }

  /**
   * Reads bytes into a buffer until EOF or the buffer's limit is reached
   */
  private int fillBuffer(FileInputStream stream, ByteBuffer buf)
      throws IOException {
    TraceScope scope = Trace.startSpan("BlockReaderLocalLegacy#fillBuffer(" +
        blockId + ")", Sampler.NEVER);
    try {
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
    } finally {
      scope.close();
    }
  }
  
  /**
   * Utility method used by read(ByteBuffer) to partially copy a ByteBuffer into
   * another.
   */
  private void writeSlice(ByteBuffer from, ByteBuffer to, int length) {
    int oldLimit = from.limit();
    from.limit(from.position() + length);
    try {
      to.put(from);
    } finally {
      from.limit(oldLimit);
    }
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    int nRead = 0;
    if (verifyChecksum) {
      // A 'direct' read actually has three phases. The first drains any
      // remaining bytes from the slow read buffer. After this the read is
      // guaranteed to be on a checksum chunk boundary. If there are still bytes
      // to read, the fast direct path is used for as many remaining bytes as
      // possible, up to a multiple of the checksum chunk size. Finally, any
      // 'odd' bytes remaining at the end of the read cause another slow read to
      // be issued, which involves an extra copy.

      // Every 'slow' read tries to fill the slow read buffer in one go for
      // efficiency's sake. As described above, all non-checksum-chunk-aligned
      // reads will be served from the slower read path.

      if (slowReadBuff.hasRemaining()) {
        // There are remaining bytes from a small read available. This usually
        // means this read is unaligned, which falls back to the slow path.
        int fromSlowReadBuff = Math.min(buf.remaining(), slowReadBuff.remaining());
        writeSlice(slowReadBuff, buf, fromSlowReadBuff);
        nRead += fromSlowReadBuff;
      }

      if (buf.remaining() >= bytesPerChecksum && offsetFromChunkBoundary == 0) {
        // Since we have drained the 'small read' buffer, we are guaranteed to
        // be chunk-aligned
        int len = buf.remaining() - (buf.remaining() % bytesPerChecksum);

        // There's only enough checksum buffer space available to checksum one
        // entire slow read buffer. This saves keeping the number of checksum
        // chunks around.
        len = Math.min(len, slowReadBuff.capacity());
        int oldlimit = buf.limit();
        buf.limit(buf.position() + len);
        int readResult = 0;
        try {
          readResult = doByteBufferRead(buf);
        } finally {
          buf.limit(oldlimit);
        }
        if (readResult == -1) {
          return nRead;
        } else {
          nRead += readResult;
          buf.position(buf.position() + readResult);
        }
      }

      // offsetFromChunkBoundary > 0 => unaligned read, use slow path to read
      // until chunk boundary
      if ((buf.remaining() > 0 && buf.remaining() < bytesPerChecksum) || offsetFromChunkBoundary > 0) {
        int toRead = Math.min(buf.remaining(), bytesPerChecksum - offsetFromChunkBoundary);
        int readResult = fillSlowReadBuffer(toRead);
        if (readResult == -1) {
          return nRead;
        } else {
          int fromSlowReadBuff = Math.min(readResult, buf.remaining());
          writeSlice(slowReadBuff, buf, fromSlowReadBuff);
          nRead += fromSlowReadBuff;
        }
      }
    } else {
      // Non-checksummed reads are much easier; we can just fill the buffer directly.
      nRead = doByteBufferRead(buf);
      if (nRead > 0) {
        buf.position(buf.position() + nRead);
      }
    }
    return nRead;
  }

  /**
   * Tries to read as many bytes as possible into supplied buffer, checksumming
   * each chunk if needed.
   *
   * <b>Preconditions:</b>
   * <ul>
   * <li>
   * If checksumming is enabled, buf.remaining must be a multiple of
   * bytesPerChecksum. Note that this is not a requirement for clients of
   * read(ByteBuffer) - in the case of non-checksum-sized read requests,
   * read(ByteBuffer) will substitute a suitably sized buffer to pass to this
   * method.
   * </li>
   * </ul>
   * <b>Postconditions:</b>
   * <ul>
   * <li>buf.limit and buf.mark are unchanged.</li>
   * <li>buf.position += min(offsetFromChunkBoundary, totalBytesRead) - so the
   * requested bytes can be read straight from the buffer</li>
   * </ul>
   *
   * @param buf
   *          byte buffer to write bytes to. If checksums are not required, buf
   *          can have any number of bytes remaining, otherwise there must be a
   *          multiple of the checksum chunk size remaining.
   * @return <tt>max(min(totalBytesRead, len) - offsetFromChunkBoundary, 0)</tt>
   *         that is, the the number of useful bytes (up to the amount
   *         requested) readable from the buffer by the client.
   */
  private synchronized int doByteBufferRead(ByteBuffer buf) throws IOException {
    if (verifyChecksum) {
      assert buf.remaining() % bytesPerChecksum == 0;
    }
    int dataRead = -1;

    int oldpos = buf.position();
    // Read as much as we can into the buffer.
    dataRead = fillBuffer(dataIn, buf);

    if (dataRead == -1) {
      return -1;
    }

    if (verifyChecksum) {
      ByteBuffer toChecksum = buf.duplicate();
      toChecksum.position(oldpos);
      toChecksum.limit(oldpos + dataRead);

      checksumBuff.clear();
      // Equivalent to (int)Math.ceil(toChecksum.remaining() * 1.0 / bytesPerChecksum );
      int numChunks =
        (toChecksum.remaining() + bytesPerChecksum - 1) / bytesPerChecksum;
      checksumBuff.limit(checksumSize * numChunks);

      fillBuffer(checksumIn, checksumBuff);
      checksumBuff.flip();

      checksum.verifyChunkedSums(toChecksum, checksumBuff, filename,
          this.startOffset);
    }

    if (dataRead >= 0) {
      buf.position(oldpos + Math.min(offsetFromChunkBoundary, dataRead));
    }

    if (dataRead < offsetFromChunkBoundary) {
      // yikes, didn't even get enough bytes to honour offset. This can happen
      // even if we are verifying checksums if we are at EOF.
      offsetFromChunkBoundary -= dataRead;
      dataRead = 0;
    } else {
      dataRead -= offsetFromChunkBoundary;
      offsetFromChunkBoundary = 0;
    }

    return dataRead;
  }

  /**
   * Ensures that up to len bytes are available and checksummed in the slow read
   * buffer. The number of bytes available to read is returned. If the buffer is
   * not already empty, the number of remaining bytes is returned and no actual
   * read happens.
   *
   * @param len
   *          the maximum number of bytes to make available. After len bytes
   *          are read, the underlying bytestream <b>must</b> be at a checksum
   *          boundary, or EOF. That is, (len + currentPosition) %
   *          bytesPerChecksum == 0.
   * @return the number of bytes available to read, or -1 if EOF.
   */
  private synchronized int fillSlowReadBuffer(int len) throws IOException {
    int nRead = -1;
    if (slowReadBuff.hasRemaining()) {
      // Already got data, good to go.
      nRead = Math.min(len, slowReadBuff.remaining());
    } else {
      // Round a complete read of len bytes (plus any implicit offset) to the
      // next chunk boundary, since we try and read in multiples of a chunk
      int nextChunk = len + offsetFromChunkBoundary +
          (bytesPerChecksum - ((len + offsetFromChunkBoundary) % bytesPerChecksum));
      int limit = Math.min(nextChunk, slowReadBuff.capacity());
      assert limit % bytesPerChecksum == 0;

      slowReadBuff.clear();
      slowReadBuff.limit(limit);

      nRead = doByteBufferRead(slowReadBuff);

      if (nRead > 0) {
        // So that next time we call slowReadBuff.hasRemaining(), we don't get a
        // false positive.
        slowReadBuff.limit(nRead + slowReadBuff.position());
      }
    }
    return nRead;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("read off " + off + " len " + len);
    }
    if (!verifyChecksum) {
      return dataIn.read(buf, off, len);
    }

    int nRead = fillSlowReadBuffer(slowReadBuff.capacity());

    if (nRead > 0) {
      // Possible that buffer is filled with a larger read than we need, since
      // we tried to read as much as possible at once
      nRead = Math.min(len, nRead);
      slowReadBuff.get(buf, off, nRead);
    }

    return nRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("skip " + n);
    }
    if (n <= 0) {
      return 0;
    }
    if (!verifyChecksum) {
      return dataIn.skip(n);
    }
  
    // caller made sure newPosition is not beyond EOF.
    int remaining = slowReadBuff.remaining();
    int position = slowReadBuff.position();
    int newPosition = position + (int)n;
  
    // if the new offset is already read into dataBuff, just reposition
    if (n <= remaining) {
      assert offsetFromChunkBoundary == 0;
      slowReadBuff.position(newPosition);
      return n;
    }
  
    // for small gap, read through to keep the data/checksum in sync
    if (n - remaining <= bytesPerChecksum) {
      slowReadBuff.position(position + remaining);
      if (skipBuf == null) {
        skipBuf = new byte[bytesPerChecksum];
      }
      int ret = read(skipBuf, 0, (int)(n - remaining));
      return (remaining + ret);
    }
  
    // optimize for big gap: discard the current buffer, skip to
    // the beginning of the appropriate checksum chunk and then
    // read to the middle of that chunk to be in sync with checksums.
  
    // We can't use this.offsetFromChunkBoundary because we need to know how
    // many bytes of the offset were really read. Calling read(..) with a
    // positive this.offsetFromChunkBoundary causes that many bytes to get
    // silently skipped.
    int myOffsetFromChunkBoundary = newPosition % bytesPerChecksum;
    long toskip = n - remaining - myOffsetFromChunkBoundary;

    slowReadBuff.position(slowReadBuff.limit());
    checksumBuff.position(checksumBuff.limit());
  
    IOUtils.skipFully(dataIn, toskip);
    long checkSumOffset = (toskip / bytesPerChecksum) * checksumSize;
    IOUtils.skipFully(checksumIn, checkSumOffset);

    // read into the middle of the chunk
    if (skipBuf == null) {
      skipBuf = new byte[bytesPerChecksum];
    }
    assert skipBuf.length == bytesPerChecksum;
    assert myOffsetFromChunkBoundary < bytesPerChecksum;

    int ret = read(skipBuf, 0, myOffsetFromChunkBoundary);

    if (ret == -1) {  // EOS
      return (toskip + remaining);
    } else {
      return (toskip + remaining + ret);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    IOUtils.cleanup(LOG, dataIn, checksumIn);
    if (slowReadBuff != null) {
      bufferPool.returnBuffer(slowReadBuff);
      slowReadBuff = null;
    }
    if (checksumBuff != null) {
      bufferPool.returnBuffer(checksumBuff);
      checksumBuff = null;
    }
    startOffset = -1;
    checksum = null;
  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  @Override
  public void readFully(byte[] buf, int off, int len) throws IOException {
    BlockReaderUtil.readFully(this, buf, off, len);
  }

  @Override
  public int available() throws IOException {
    // We never do network I/O in BlockReaderLocalLegacy.
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean isLocal() {
    return true;
  }
  
  @Override
  public boolean isShortCircuit() {
    return true;
  }

  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }
}
