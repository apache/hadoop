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
package org.apache.hadoop.dfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.*;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.concurrent.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;

/* This containtains information about CRC file and blocks created by
 * ChecksumFileSystem for a given block.
 */
class BlockCrcInfo implements Writable {
  
  public static final int STATUS_DATA_BLOCK = 0;
  public static final int STATUS_CRC_BLOCK = 1; // block belongs a .crc file
  public static final int STATUS_NO_CRC_DATA = 2;
  public static final int STATUS_UNKNOWN_BLOCK = 3;
  public static final int STATUS_ERROR = 4; // unknown error
  
  int status = STATUS_ERROR;

  String fileName = "";
  long fileSize;
  long startOffset;
  long blockLen;
  
  LocatedBlock[] crcBlocks; // actual block locations.
  int crcReplication; // expected replication.
  
  // set during 'offline upgrade'
  boolean blockLocationsIncluded;
  LocatedBlock blockLocations = new LocatedBlock(); 
  
  private void writeBlockLocations(LocatedBlock[] locArr, DataOutput out) 
                                   throws IOException {
    int len = ( locArr == null ) ? 0 : locArr.length;
    out.writeInt( len );
    if ( len > 0 ) {
      for (LocatedBlock loc : locArr) {
        loc.write( out );
      }
    }    
  }
  
  private LocatedBlock[] readBlockLocations(DataInput in) throws IOException {
    int len = in.readInt();
    LocatedBlock[] locArr = (len > 0) ? new LocatedBlock[len] : null; 
    for (int i=0; i<len; i++) {
      locArr[i] = new LocatedBlock();
      locArr[i].readFields( in );
    }
    return locArr;
  }
  
  // Writable interface
  
  public void write(DataOutput out) throws IOException {
    out.writeInt( status );
    Text.writeString( out, fileName );
    out.writeLong( fileSize );
    out.writeLong( startOffset );
    out.writeLong( blockLen );
    writeBlockLocations(crcBlocks, out);
    out.writeInt(crcReplication);
    out.writeBoolean(blockLocationsIncluded);
    blockLocations.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    status = in.readInt();
    fileName = Text.readString( in );
    fileSize = in.readLong();
    startOffset = in.readLong();
    blockLen = in.readLong();
    crcBlocks = readBlockLocations( in );
    crcReplication = in.readInt();
    blockLocationsIncluded = in.readBoolean();
    blockLocations.readFields( in );
  }
}

/**
 * Consolidates various information regd upgrade of a single block at the
 * Datanode.
 */
class DNBlockUpgradeInfo {
  
  Block block;
  DataNode dataNode;
  DatanodeProtocol namenode;
  
  BlockCrcInfo crcInfo; // CrcInfo fetched from the namenode.
  boolean offlineUpgrade;
  
  /** Returns string that has block id and the associated file */
  @Override
  public String toString() {
    return block + " (filename: " +
           ( (crcInfo == null || crcInfo.fileName == null) ? 
             "Unknown" : crcInfo.fileName ) + ")";
  }
}

/**
 * This class contains various utilities for upgrade of DFS during switch
 * to block level CRCs (HADOOP-1134).
 */
class BlockCrcUpgradeUtils {
  
  public static final Log LOG = 
    LogFactory.getLog("org.apache.hadoop.dfs.BlockCrcUpgradeUtils");
  
  /* If some operation does not finish in this time, mostly something
   * has gone wrong in the cluster (mostly it would be wrong configuration).
   */ 
  public static final int LONG_TIMEOUT_MINUTES = 5;
  public static final int LONG_TIMEOUT_MILLISEC = LONG_TIMEOUT_MINUTES*60*1000;
  public static final int PRE_BLOCK_CRC_LAYOUT_VERSION = -6;
  
  /** 
   * Reads block data from a given set of datanodes. It tries each
   * datanode in order and tries next datanode if fetch from prev datanode
   * fails.
   * 
   * @param loc locations of the block
   * @param offset offset into the block
   * @param len len of the data to be read from the given offset
   * @param buf buffer for data
   * @param bufOffset offset into the buffer for writing
   * @throws IOException
   */ 
  static void readFromRemoteNode(LocatedBlock loc, String crcFile,
                                 long offset, long len,
                                 byte[] buf, int bufOffset) 
                                 throws IOException {
    Block blk = loc.getBlock();
    String dnStr = "";
    
    for (DatanodeInfo dn : loc.getLocations()) {
      dnStr += dn.getName() + " ";
      Socket dnSock = null;
      try {
        InetSocketAddress dnAddr = NetUtils.createSocketAddr(dn.getName());
        dnSock = SocketChannel.open().socket();
        dnSock.connect(dnAddr, FSConstants.READ_TIMEOUT);
        dnSock.setSoTimeout(FSConstants.READ_TIMEOUT);
        DFSClient.BlockReader reader = DFSClient.BlockReader.newBlockReader
                    (dnSock, crcFile, blk.getBlockId(), offset, len, 
                     (int)Math.min(len, 4096));
        IOUtils.readFully(reader, buf, bufOffset, (int)len);
        return;
      } catch (IOException ioe) {
        LOG.warn("Could not read " + blk + " from " + dn.getName() + " : " +
                 StringUtils.stringifyException(ioe));
      } finally {
        if ( dnSock != null )
          dnSock.close();
      }
    }
    
    throw new IOException("Could not fetch data for " + blk +
                          " from datanodes " + dnStr);
  }
   
  /**
   * Reads data from an HDFS file from a given file-offset. 
   * Before opening the file, it forces "fs.hdfs.impl" to
   * "ChecksumDistributedFileSystem". So this is meant to read
   * only files that have associcated ".crc" files.
   * 
   * @param filename HDFS complete filename for this file. 
   * @param fileOffset fileOffset to read from
   * @param len length of the data to read.
   * @param namenodeAddr Namenode address for creating "hdfs://..." path.
   * @param buf buffer to read into
   * @param bufOffset offset into read buffer.
   * @throws IOException
   */
  static void readDfsFileData(String filename, long fileOffset, int len, 
                              InetSocketAddress namenodeAddr,
                              byte[] buf, int bufOffset)
                              throws IOException {
    /*
     * Read from an HDFS file.
     */
    Configuration conf = new Configuration();
    //Makesure we use ChecksumFileSystem.
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.dfs.ChecksumDistributedFileSystem");
    URI uri;
    try {
      uri = new URI("hdfs://" + namenodeAddr.getHostName() + ":" +
                    namenodeAddr.getPort());
    } catch (URISyntaxException e) {
      throw new IOException("Got URISyntaxException for " + filename);
    }
    
    FileSystem fs = FileSystem.get(uri, conf);
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(filename));
      if (fileOffset > 0) {
        in.seek(fileOffset);
      }
      in.readFully(buf, bufOffset, len);
    } finally {
      IOUtils.closeStream(in);
    }
  }
  
  
  /**
   * This method is the main entry point for fetching CRC data for  
   * a block from corresponding blocks for ".crc" file. 
   * <br><br>
   * 
   * It first reads header from the ".crc" file and then invokes 
   * readCrcBuf() to read the actual CRC data.
   * It then writes the checksum to disk.
   *
   * 
   * @param blockInfo
   * @throws IOException when it fails to fetch CRC data for any reason.
   */
  static void readCrcFileData(DNBlockUpgradeInfo blockInfo)
                              throws IOException {
    
    //First read the crc header  
    byte[] header = new byte[8]; //'c r c \0 int '
    int bytesRead = 0;
    BlockCrcInfo crcInfo = blockInfo.crcInfo;
    
    for(int i=0; i<crcInfo.crcBlocks.length && 
                 bytesRead < header.length; i++) {
      
      LocatedBlock loc = crcInfo.crcBlocks[i];
      long toRead = Math.min(loc.getBlock().getNumBytes(),
                             header.length-bytesRead);
      readFromRemoteNode(loc, "."+crcInfo.fileName+".crc", 
                         0, toRead, header, bytesRead);
      bytesRead += toRead;
    }
    
    if (bytesRead != header.length || header[0] != 'c' || 
        header[1] != 'r' || header[2] != 'c' || header[3] != 0) {
      // Should be very rare.
      throw new IOException("Could not read header from crc file");
    }
    
    int bytesPerChecksum = ((header[4] & 0xff) << 24) |
                           ((header[5] & 0xff) << 16) |
                           ((header[6] & 0xff) << 8) |
                           (header[7] & 0xff);
    
    // sanity check the value. Is 100 MB good reasonable upper limt?
    if (bytesPerChecksum < 1 || bytesPerChecksum > 100*1024*1024) {
      throw new IOException("Insane value for bytesPerChecksum (" +
                            bytesPerChecksum + ")");
    }
    
    byte[] crcBuf = null;    
        
    try {
      crcBuf = readCrcBuf(blockInfo, bytesPerChecksum);
    } catch (IOException ioe) {
      LOG.warn("Failed to fetch CRC data for " + blockInfo);
      throw ioe;
    } 
    
    writeCrcData(blockInfo, bytesPerChecksum, crcBuf);
    /* After successful write(), we could inform the name node about it.
     * or we can just inform name after all the blocks have been upgraded.
     * Waiting for all the blocks to complete is probably better.
     */
  }
  
  /** 
   * Low level function to create metadata file for a block with the
   * CRC data. If crcBuf is null, it writes a metadata file with empty checksum.
   * 
   * @param blockInfo
   * @param bytesPerChecksum 
   * @param crcBuf buffer containing CRC. null implies metadata file should be
   *        written with empty checksum.
   * @throws IOException
   */
  static void writeCrcData(DNBlockUpgradeInfo blockInfo, int bytesPerChecksum,
                           byte[] crcBuf) throws IOException {
    Block block = blockInfo.block;
    
    FSDataset data = (FSDataset) blockInfo.dataNode.data;
    File blockFile = data.getBlockFile( block );
    File metaFile = FSDataset.getMetaFile( blockFile );
    
    if ( bytesPerChecksum <= 0 ) {
      if (crcBuf == null) {
        bytesPerChecksum = blockInfo.dataNode.defaultBytesPerChecksum;
      } else {
        throw new IOException("Illegal Argument bytesPerChecksum(" +
                              bytesPerChecksum);
      }
    }
    
    if ( metaFile.exists() ) {
      if ( true ) {
        throw new IOException("metadata file exists but unexpected for " +
                              blockInfo);
      }
      // Verify the crcBuf. this should be removed.
      if ( crcBuf == null )  {
        return;
      }
      FileInputStream in = null;
      try {
        in = new FileInputStream( metaFile );
        in.skip(7); //should be skipFully().
        byte[] storedChecksum = new byte[ crcBuf.length ];
        IOUtils.readFully(in, storedChecksum, 0, storedChecksum.length);
        if ( !Arrays.equals(crcBuf, storedChecksum) ) {
          throw new IOException("CRC does not match");
        }
      } finally {
        IOUtils.closeStream(in);
      }
      return;
    }
    
    File tmpBlockFile = null;
    File tmpMetaFile = null;
    DataOutputStream out = null;
    try {
      tmpBlockFile = data.createTmpFile(null, block);
      tmpMetaFile = FSDataset.getMetaFile( tmpBlockFile );
      out = new DataOutputStream( new FileOutputStream(tmpMetaFile) );
      
      // write the header
      out.writeShort( FSDataset.METADATA_VERSION );  
      DataChecksum checksum = 
        DataChecksum.newDataChecksum( ((crcBuf == null) ?
                                       DataChecksum.CHECKSUM_NULL :
                                       DataChecksum.CHECKSUM_CRC32 ),
                                       bytesPerChecksum );
      checksum.writeHeader(out);
      if (crcBuf != null) {
        out.write(crcBuf);
      }
      out.close();
      out = null;
      
      if ( !tmpMetaFile.renameTo( metaFile ) ) {
        throw new IOException("Could not rename " + tmpMetaFile + " to " +
                              metaFile);
      }
    } finally {
      IOUtils.closeStream(out);
      if ( tmpBlockFile != null ) {
        tmpBlockFile.delete();
      }
    }
  }
  
  /**
   * This regenerates CRC for a block by checksumming few bytes at either end 
   * of the block (read from the HDFS file) and the local block data. This is 
   * invoked only when either end of the block  doesn't fall on checksum
   * boundary (very uncommon). <i>bytesBefore</i> or <i>bytesAfter</i>
   * should have non-zero length.
   *  
   * @param blockInfo
   * @param oldCrcBuf CRC buffer fetched from ".crc" file
   * @param bytesPerChecksum
   * @param bytesBefore bytes located before this block in the file
   * @param bytesAfter bytes located after this block in the file
   * @return Returns the generated CRC in a buffer.
   * @throws IOException
   */
  static byte[] regenerateCrcBuf(DNBlockUpgradeInfo blockInfo,
                                 byte[] oldCrcBuf, int bytesPerChecksum,
                                 byte[] bytesBefore, byte[] bytesAfter)
                                 throws IOException {
    
    DataChecksum verificationChecksum = DataChecksum.newDataChecksum
                                         (DataChecksum.CHECKSUM_CRC32, 
                                          bytesPerChecksum);
    DataChecksum newChecksum = DataChecksum.newDataChecksum
                                         (DataChecksum.CHECKSUM_CRC32, 
                                          bytesPerChecksum);
    
    BlockCrcInfo crcInfo = blockInfo.crcInfo;
    
    int checksumSize = newChecksum.getChecksumSize();
    int newCrcSize = (int) (crcInfo.blockLen/bytesPerChecksum*checksumSize);
    if ( crcInfo.blockLen%bytesPerChecksum > 0 ) {
      newCrcSize += checksumSize;
    }
    byte[] newCrcBuf = new byte[newCrcSize];
    int newCrcOffset = 0;
    int oldCrcOffset = 0;
    
    Block block = blockInfo.block;
    FSDataset data = (FSDataset) blockInfo.dataNode.data;
    File blockFile = data.getBlockFile( block );
    if ( blockFile == null || !blockFile.exists() ) {
      throw new IOException("Block file "  + 
                            ((blockFile != null) ? blockFile.getAbsolutePath()
                              : "NULL") + " does not exist.");
    }
    
    byte[] blockBuf = new byte[bytesPerChecksum];
    FileInputStream in = null;
       
    try {
      boolean chunkTainted = false;
      boolean prevChunkTainted = false;
      long bytesRead = 0;

      if ( bytesBefore.length > 0 ) {
        verificationChecksum.update(bytesBefore, 0, bytesBefore.length);
      }
      int verifyLen = bytesPerChecksum - bytesBefore.length;
      long verifiedOffset = -bytesBefore.length;
      long writtenOffset = 0;

      in = new FileInputStream( blockFile );
      
      while ( bytesRead <= crcInfo.blockLen ) {        
        /* In each iteration we read number of bytes required for newChecksum,
         * except in the last iteration where we read 0 bytes.
         * newChecksum updated in an iteration is written in the next 
         * iteration, because only in the next iteration will we be 
         * gauranteed that we have verified enough data to write newChecksum.
         * All the comparisions below are chosen to enforce the above.
         */
        int toRead = (int) Math.min(crcInfo.blockLen - bytesRead, 
                                    bytesPerChecksum);
        /* if bytesBefore.length == 0, then we need not rechecksum but
         * simply copy from oldCrcBuf for most of the block.
         * But we are not optimizing for this case.
         */
        if ( toRead > 0 ) {
          IOUtils.readFully(in, blockBuf, 0, toRead);
        }

        if ( (toRead == 0 && bytesAfter.length > 0) || toRead >= verifyLen ) {
          if ( toRead > 0 ) {
            verificationChecksum.update(blockBuf, 0, verifyLen);
          }
          prevChunkTainted = chunkTainted;
          chunkTainted = !verificationChecksum.compare(oldCrcBuf, oldCrcOffset);
          oldCrcOffset += checksumSize;
          verifiedOffset += bytesPerChecksum;
          verificationChecksum.reset();
        }

        /* We update newCrcBuf only after all the bytes checksummed are
         * verified.
         */
        long diff = verifiedOffset - writtenOffset;
        if ( toRead == 0 || diff > bytesPerChecksum || 
            bytesRead >= verifiedOffset ) {
          // decide if we need to reset the checksum.
          if ( (diff > bytesPerChecksum && prevChunkTainted) || 
              (diff < 2L*bytesPerChecksum && chunkTainted) ) {
            LOG.warn("Resetting checksum for " + blockInfo + " at offset "
                     + writtenOffset);
            newChecksum.reset();
          }
          newChecksum.writeValue(newCrcBuf, newCrcOffset, true);
          newCrcOffset += checksumSize;
          writtenOffset += Math.min( crcInfo.blockLen-writtenOffset,
                                     bytesPerChecksum );
        }

        if ( toRead == 0 ) {
          //Finally done with update the new CRC buffer!
          break;
        }

        if ( toRead != verifyLen ) {
          int tmpOff = ( toRead > verifyLen ) ? verifyLen : 0; 
          verificationChecksum.update(blockBuf, tmpOff, toRead-tmpOff);
        }

        bytesRead += toRead;
        if ( bytesRead == crcInfo.blockLen && bytesAfter.length > 0 ) {
          /* We are at the edge.
           * if bytesBefore.length == 0, then blockLen % bytesPerChecksum
           * can not be 0.
           */
          verificationChecksum.update(bytesAfter, 0, bytesAfter.length);
        }
        newChecksum.update(blockBuf, 0, toRead);
      }

      //XXX Remove the assert.
      assert newCrcBuf.length == newCrcOffset : "something is wrong"; 
      return newCrcBuf;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Reads multiple copies of CRC data from different replicas and  
   * compares them. It selects the CRC data that matches on majority
   * of the copies. If first 'replication/2+1' copies match with each
   * other it does not read rest of the copies of CRC data, this would be
   * the common case for most the blocks.
   * <br><br>
   * 
   * When CRC data falls on multiple blocks, the permutations it tries are 
   * pretty simple. If there 2 blocks involved, with replication of 3, it tries
   * 3 combinations and not 9. Currently it does not read more replicas
   * if CRC data is over-replicated, reads only up to file's 'replication'.
   * <br><br> 
   * 
   * If all copies are different, it returns the first one.
   * <br>
   * 
   * @param blockInfo
   * @param crcStart start offset into the ".crc" file
   * @param crcSize length of the CRC data
   * @return Returns buffer containing crc data. buffer.length == crcSize
   * @throws IOException when it can not fetch even one copy of the CRC data.
   */
  static byte[] findMajorityCrcBuf(DNBlockUpgradeInfo blockInfo,
                                   long crcStart, int crcSize)
                                   throws IOException {
    
    Block block = blockInfo.block;
    BlockCrcInfo crcInfo = blockInfo.crcInfo;
    int replication = crcInfo.crcReplication;
    
    // internal/local class
    class CrcBufInfo {
      CrcBufInfo(byte[] b, long checksum) {
        buf = b;
        crc32 = checksum;
      }
      byte[] buf;
      long crc32;
      int  numMatches = 0;
    }
    
    CrcBufInfo[] bufInfoArr = new CrcBufInfo[replication];
    int numBufs = 0;
    CRC32 crc32 = new CRC32();
    boolean atleastOneNewReplica = true;
    
    for(int i=0; i<replication && atleastOneNewReplica; i++) {
      /* when crc data falls on multiple blocks, we don't
       * try fetching from all combinations of the replicas. We could.
       * We try only crcReplication combinations.
       */
      int bytesRead = 0;
      byte[] buf = new byte[crcSize];
      long offset = 0;
      atleastOneNewReplica = false;
      String crcFileName = "."+blockInfo.crcInfo.fileName+".crc";
      for (LocatedBlock loc : crcInfo.crcBlocks) {          

        long blockStart = crcStart - offset + bytesRead; 
        long blockSize = loc.getBlock().getNumBytes();
        offset += blockSize;
        if ( blockSize <= blockStart ){
          continue;
        }
        
        DatanodeInfo dn;
        DatanodeInfo [] dnArr = loc.getLocations();
        if ( dnArr.length > i ) {
          dn = dnArr[i];
          atleastOneNewReplica = true;
        } else {
          // if all the data is in single block, then no need to read
          if ( (bytesRead == 0 || !atleastOneNewReplica) &&
               (blockSize - blockStart) >= (crcSize - bytesRead) ) {
            break;
          }
          dn = dnArr[dnArr.length-1];
        }
        
        try {
          DatanodeInfo[] tmpArr = new DatanodeInfo[1];
          tmpArr[0] = dn;
          long toRead = Math.min(crcSize - bytesRead, blockSize-blockStart);
          readFromRemoteNode(new LocatedBlock(loc.getBlock(), tmpArr),
                             crcFileName, blockStart, toRead, buf, bytesRead);
          bytesRead += toRead;
        } catch (IOException ioe) {
          LOG.warn("Error while fetching crc data from " + dn.getName() +
                   "for " + blockInfo + StringUtils.stringifyException(ioe));
          break;
        }
        
        if ( bytesRead < crcSize ) {
          continue;
        }
        
        crc32.reset();
        crc32.update(buf, 0, crcSize);
        long crc = crc32.getValue();

        for(int j=0; j<numBufs+1; j++) {
          if ( j < numBufs && crc != bufInfoArr[j].crc32 ) {
            LOG.warn("Mismatch in crc for " + blockInfo);
            continue;
          }

          CrcBufInfo info = ( j < numBufs ) ? bufInfoArr[j] :
            new CrcBufInfo(buf, crc);
          
          info.numMatches++;
          if (info.numMatches >= (replication/2 + replication%2)) {
            LOG.info("At least " + info.numMatches + 
                     " of the " + replication + 
                     " replicated CRC files agree for " + blockInfo);
            return buf;
          }

          if ( j == numBufs ) {
            bufInfoArr[ numBufs++ ] = info;
          }
          break;
        }
        
        // Done reading crcSize bytes.
        break;
      }
    }
    
    /* Now we have an error or some buffer that might not have 
     * absolute majority.
     * Try to pick the buffer that that has max number of matches.
     */
    int replicasFetched = 0;
    CrcBufInfo selectedBuf = null;
    for (int i=0; i<numBufs; i++) {
      CrcBufInfo info = bufInfoArr[i];
      replicasFetched += info.numMatches;
      if (selectedBuf == null || selectedBuf.numMatches < info.numMatches) {
        selectedBuf = info;
      }
    }

    if (selectedBuf == null) {      
      throw new IOException("Could not fetch any crc data for " + block);
    }

    LOG.info(selectedBuf.numMatches + " of the " + replicasFetched + 
             " CRC replicas fetched agree for " + blockInfo);
    
    //Print a warning if numMatches is 1?
    return  selectedBuf.buf;      
  }
  
  /**
   * Reads CRC data for a block from corresponding ".crc". Usually it 
   * fetches CRC from blocks that belong to ".crc" file. When the given 
   * block does not start or end on a checksum boundary, it would read data
   * from ".crc" DFS itself (as opposed to directly fetching from blocks) and
   * regenerate CRC for the block.
   * 
   * @param blockInfo
   * @param bytesPerChecksum bytesPerChecksum from ".crc" file header 
   * @return buffer containing CRC data for the block.
   * @throws IOException
   */
  static byte[] readCrcBuf(DNBlockUpgradeInfo blockInfo, int bytesPerChecksum)
                           throws IOException {
    
    BlockCrcInfo crcInfo = blockInfo.crcInfo;
    
    int checksumSize = 4; // CRC32.
    
    /* Following two arrayas are used in the case where block 
     * does not fall on edges. This happens when 'dfs.block.size' is not
     * a multiple of 'io.bytes.per.checksum'. Apparently it can also happen
     * because of a known bug in DFSClient, but I am not sure. 
     */
    byte [] bytesBefore = new byte[(int)(crcInfo.startOffset % 
                                         bytesPerChecksum)]; 
    int tmpMod = (int) ((crcInfo.blockLen + bytesBefore.length) % 
                        bytesPerChecksum);
    if ( tmpMod != 0 ) {
      tmpMod = (int) Math.min(bytesPerChecksum - tmpMod,
                              (crcInfo.fileSize - crcInfo.startOffset - 
                               crcInfo.blockLen));
    }
    byte [] bytesAfter = new byte[tmpMod];
    
    if ( bytesBefore.length > 0 || bytesAfter.length > 0 ) {
      if ( bytesBefore.length > 0 ) {
        readDfsFileData(crcInfo.fileName,
                        crcInfo.startOffset-bytesBefore.length,
                        bytesBefore.length,
                        blockInfo.dataNode.getNameNodeAddr(),
                        bytesBefore, 0);
      }
      if ( bytesAfter.length > 0 ) {
        readDfsFileData(crcInfo.fileName,  
                        crcInfo.startOffset+crcInfo.blockLen, 
                        bytesAfter.length,
                        blockInfo.dataNode.getNameNodeAddr(),                        
                        bytesAfter, 0);
      }
    }
    
    // Now fetch the crc. XXX change 8 to HEADER_SIZE.
    long crcStart = 8 + ( (crcInfo.startOffset-bytesBefore.length)/
                          bytesPerChecksum * checksumSize );
    
    long tmpLen = crcInfo.blockLen + bytesBefore.length + bytesAfter.length;
    int crcSize = (int) (tmpLen/bytesPerChecksum*checksumSize);
    if (tmpLen % bytesPerChecksum > 0) {
      crcSize += checksumSize;
    }
    
    byte[] crcBuf = findMajorityCrcBuf(blockInfo, crcStart, crcSize);
    
    if ( bytesBefore.length > 0 || bytesAfter.length > 0 ) {
      /* We have crc for data that is larger than the blocks data.
       * So regenerate new CRC data.
       */
      crcBuf = regenerateCrcBuf(blockInfo, crcBuf, 
                                bytesPerChecksum, bytesBefore, bytesAfter);
    }
    return crcBuf;
  }
  
  /** Generates CRC data for a block by reading block data from the local
   * storage. Usually this is used as the last resort option to generate
   * metadata for a block.
   * <br><br>
   * 
   * Bytes per checksum is based on config (io.bytes.per.checksum).
   * 
   * @param blockInfo Block information.
   * @throws IOException
   */
  static void generateLocalCrcData( DNBlockUpgradeInfo blockInfo ) 
                                   throws IOException {
    
    Block block = blockInfo.block;
    FSDataset data = (FSDataset) blockInfo.dataNode.data;
    File blockFile = data.getBlockFile( block );
    if (blockFile == null || !blockFile.exists()) {
      throw new IOException("Could not local file for block");
    }
    long blockLen = blockFile.length();
    if ( blockLen != blockFile.length()) {
      LOG.warn("Mismatch in length for block: local file size is " +
               blockLen + " but should be " + blockInfo.crcInfo.blockLen +
               " for " + blockInfo + ". Using local file size for CRC");
    }
    int bytesPerChecksum = blockInfo.dataNode.defaultBytesPerChecksum;
    DataChecksum checksum = 
      DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
                                   bytesPerChecksum);
    int checksumSize = checksum.getChecksumSize();
    int crcBufSize = (int) ((blockLen/bytesPerChecksum)*checksumSize);
    if (blockLen%bytesPerChecksum != 0) {
      crcBufSize += checksumSize;
    }
    byte crcBuf[] = new byte[crcBufSize];

    
    InputStream in = null;
    try {
      in = new FileInputStream(blockFile);
      
      long totalRead = 0;
      byte[] dataBuf = new byte[bytesPerChecksum];
      int crcBufPos = 0;     
      
      while (totalRead < blockLen) {
        int toRead = Math.min((int)(blockLen - totalRead), bytesPerChecksum);
        IOUtils.readFully(in, dataBuf, 0, toRead );
        
        checksum.update(dataBuf, 0, toRead);
        crcBufPos += checksum.writeValue(crcBuf, crcBufPos, true);
        checksum.reset();
        
        totalRead += toRead;
      }
    } finally {
      IOUtils.closeStream(in);
    }
    
    writeCrcData(blockInfo, bytesPerChecksum, crcBuf);
  }
    
  /**
   * Fetches CRC data from a remote node. Sends READ_BLOCK_METADATA
   * command. Extracts CRC information and returns it.
   */
  static byte[] readCrcFromReplica(DNBlockUpgradeInfo blockInfo,
                                   DatanodeInfo dnInfo,
                                   DataChecksum[] checksumArr
                                   ) throws IOException {
    Socket dnSock = null;
    
    String errMsg = "";
    
    try {
      do {
        InetSocketAddress dnAddr = NetUtils.createSocketAddr(dnInfo.getName());
        dnSock = new Socket();
        dnSock.connect(dnAddr, FSConstants.READ_TIMEOUT);
        dnSock.setSoTimeout(FSConstants.READ_TIMEOUT);

        DataOutputStream out = new DataOutputStream(dnSock.getOutputStream());
        DataInputStream in = new DataInputStream(dnSock.getInputStream());

        // Write the header:
        out.writeShort( DataNode.DATA_TRANSFER_VERSION );
        out.writeByte( DataNode.OP_READ_METADATA );
        out.writeLong( blockInfo.block.getBlockId() );

        byte reply = in.readByte();
        if ( reply != DataNode.OP_STATUS_SUCCESS ) {
          errMsg = "Got error(" + reply + ") in reply";
          break;
        }

        // There is no checksum for this transfer.
        int len = in.readInt();
        int headerLen = 2 + DataChecksum.HEADER_LEN;
        
        if ( len < headerLen ) {
          errMsg = "len is too short";
          break;
        }
        
        if ( len > 0 ) {
          // Verify that version is same
          short version = in.readShort();
          if ( version != FSDataset.METADATA_VERSION ) {
            errMsg = "Version mismatch";
            break;
          }

          DataChecksum checksum = DataChecksum.newDataChecksum(in);
          
          if ( checksum.getChecksumType() != DataChecksum.CHECKSUM_CRC32 ) {
            errMsg = "Checksum is not CRC32";
            break;
          }
          
          int crcBufLen = (int) (( (blockInfo.crcInfo.blockLen + 
                                    checksum.getBytesPerChecksum()-1)/
                                    checksum.getBytesPerChecksum() ) *
                                  checksum.getChecksumSize());
          
          if ( (len - headerLen) != crcBufLen ) {
            errMsg = "CRC data is too short";
          }
          byte[] crcBuf = new byte[crcBufLen];
          in.readFully(crcBuf);
          
          //just read the last int
          in.readInt();
          
          checksumArr[0] = checksum;
          return crcBuf;
        }
      } while (false);
    } finally {
      IOUtils.closeSocket( dnSock );
    }
    
    throw new IOException("Error while fetching CRC from replica on " +
                          dnInfo.getName() + ": " + errMsg); 
  }
    
    
  /**
   * Reads metadata from the replicas and writes the CRC from the
   * first successful fetch.
   */
  static void readCrcFromReplicas(DNBlockUpgradeInfo blockInfo) 
                                  throws IOException {
    
    /* Reads metadata from from the replicas */
    DatanodeInfo[] dnArr = ( blockInfo.crcInfo.blockLocationsIncluded ? 
                             blockInfo.crcInfo.blockLocations.getLocations() :
                            new DatanodeInfo[0]);
    
    DataChecksum[] checksumArr = new DataChecksum[1];
    IOException ioe = null;    
    String myName = blockInfo.dataNode.dnRegistration.getName();
    
    for (DatanodeInfo dnInfo : dnArr) {
      if ( dnInfo.getName().equals(myName) ) {
        LOG.info("skipping crcInfo fetch from " + dnInfo.getName());
      } else {
        try {
          byte[] crcBuf = readCrcFromReplica(blockInfo, dnInfo, checksumArr);
          LOG.info("read crcBuf from " + dnInfo.getName() + " for " +
                   blockInfo);
          
          writeCrcData(blockInfo, checksumArr[0].getBytesPerChecksum(), 
                       crcBuf);
          return;
        } catch (IOException e) {
          LOG.warn("Could not fetch crc data from " + dnInfo.getName() +
                   " : " + e);
          ioe = e;
        }
      }
    }
    
    if ( ioe != null ) {
      throw ioe;
    }
    
    throw new IOException("Could not fetch crc data from any node");
  }
  
  
  /**
   * The method run by the upgrade threads. It contacts namenode for 
   * information about the block and invokes appropriate method to create 
   * metadata file for the block.
   * 
   * @param blockInfo
   */
  static void upgradeBlock( DNBlockUpgradeInfo blockInfo ) {

    UpgradeCommand ret = null;
    
    if ( blockInfo.offlineUpgrade ) {
      blockInfo.crcInfo = getBlockLocations(blockInfo.namenode, 
                                            blockInfo.block);
    } else { 
      ret = sendCommand(blockInfo.namenode, new CrcInfoCommand(blockInfo.block),
                        -1);
    }
    
    /* Should be removed.
    if (true) {
      int sleepTime = (new Random(blockInfo.block.getBlockId())).nextInt(10);
       BlockCrcUpgradeUtils.sleep(sleepTime,"XXX before upgrading the block");
    } */
    
    try {
     
      if ( !blockInfo.offlineUpgrade ) {
        if ( ret == noUpgradeOnNamenode ) {
          throw new IOException("No upgrade is running on Namenode");
        }

        if ( ret == null || ((CrcInfoCommandReply)ret).crcInfo == null ) {
          throw new IOException("Could not get crcInfo from Namenode");
        }

        blockInfo.crcInfo = ((CrcInfoCommandReply)ret).crcInfo;
      }
      
      if ( blockInfo.crcInfo == null ) {
        throw new IOException("Could not fetch crcInfo for " + 
                              blockInfo.block);
      }
      
      switch (blockInfo.crcInfo.status) {
      
      case BlockCrcInfo.STATUS_DATA_BLOCK:
        try {
          if (blockInfo.offlineUpgrade) {
            readCrcFromReplicas(blockInfo);
          } else {
            readCrcFileData(blockInfo);
          }
        } catch (IOException e) {
          LOG.warn("Exception in " + 
                   ((blockInfo.offlineUpgrade) ? 
                    "readCrcFromReplicas()" : "readCrcFileData()") + 
                   " for " + blockInfo + ". will try to generate local crc.");
          throw e;
        }
        break;
        
      case BlockCrcInfo.STATUS_NO_CRC_DATA:
        generateLocalCrcData(blockInfo);
        break;
        
      case BlockCrcInfo.STATUS_UNKNOWN_BLOCK:
        LOG.info("block is already deleted. Will create an empty " +
                 "metadata file for " + blockInfo);
        writeCrcData(blockInfo, 0, null);
        break;
        
      case BlockCrcInfo.STATUS_CRC_BLOCK :
        writeCrcData(blockInfo, 0, null);
        break;
        
      case BlockCrcInfo.STATUS_ERROR:
        LOG.info("unknown error. will generate local crc data for " + 
                 blockInfo);
        generateLocalCrcData(blockInfo);
        break;
        
      default:
        LOG.error("Unknown status from Namenode for " + blockInfo);
        assert false : "Unknown status from Namenode";
      }
    } catch (IOException e) {
      LOG.warn("Could not fetch crc for " + blockInfo + 
               " will generate local crc data : exception :" + 
               StringUtils.stringifyException(e));
      try {
        // last option:
        generateLocalCrcData(blockInfo);
      } catch (IOException ioe) {
        LOG.warn("Could not generate local crc data for " + blockInfo +
                 " : exception : " + StringUtils.stringifyException(ioe));
      }
    }
  }

  static UpgradeCommand noUpgradeOnNamenode = new UpgradeCommand();
  
  /** 
   * retries command in case of timeouts. <br>
   * If retries is < 0, retries forever. <br>
   * NOTE: Does not throw an exception. <br>
   */
  static UpgradeCommand sendCommand(DatanodeProtocol namenode,
                                    UpgradeCommand cmd, int retries) {
    for(int i=0; i<=retries || retries<0; i++) {
      try {
        UpgradeCommand reply = namenode.processUpgradeCommand(cmd);
        if ( reply == null ) {
          /* namenode might not be running upgrade or finished
           * an upgrade. We just return a static object */
          return noUpgradeOnNamenode;
        }
        return reply;
      } catch (IOException e) {
        // print the stack trace only for the last retry.
        LOG.warn("Exception while sending command " + 
                 cmd.getAction() + ": " + e +
                 ((retries<0 || i>=retries)? "... will retry ..." : 
                   ": " + StringUtils.stringifyException(e)));
      }
    }
    return null; 
  }
  
  // Similar to sendCommand(). Invokes command in a loop.
  static BlockCrcInfo getBlockLocations(DatanodeProtocol namenode, 
                                        Block block) {
    for (;;) {
      try {
        return namenode.blockCrcUpgradeGetBlockLocations(block);
      } catch (IOException e) {
        LOG.warn("Exception while fetching block Locations from namenode: " +
                 e + " ... will retry ...");
      }
    }
  }
  
    
  /** sleep method that catches and swallows InterruptedException
   */
  static void sleep(int seconds, String message) {
    if ( message != null ) {
      LOG.info("XXX Sleeping for " + seconds + " seconds. msg: " + message);
    }
    try {
      Thread.sleep(seconds*1000L);
    } catch (InterruptedException ignored) {}
  }
  
  /* Upgrade commands */
  static final int DN_CMD_STATS = 200;
  static final int DN_CMD_CRC_INFO = 201;
  static final int DN_CMD_CRC_INFO_REPLY = 202;
  
  // what is this version for?
  static final int DN_CMD_VERSION = PRE_BLOCK_CRC_LAYOUT_VERSION;

  static class DatanodeStatsCommand extends UpgradeCommand {
    DatanodeID datanodeId;
    int blocksUpgraded;
    int blocksRemaining;
    int errors;
    
    DatanodeStatsCommand() {
      super(DN_CMD_STATS, DN_CMD_VERSION, (short)0);
      datanodeId = new DatanodeID();
    }
    
    public DatanodeStatsCommand(short status, DatanodeID dn,
                                int blocksUpgraded, int blocksRemaining,
                                int errors) {
      super(DN_CMD_STATS, DN_CMD_VERSION, status);
      
      //copy so that only ID part gets serialized
      datanodeId = new DatanodeID(dn); 
      this.blocksUpgraded = blocksUpgraded;
      this.blocksRemaining = blocksRemaining;
      this.errors = errors;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      datanodeId.readFields(in);
      blocksUpgraded = in.readInt();
      blocksRemaining = in.readInt();
      errors = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      datanodeId.write(out);
      out.writeInt(blocksUpgraded);
      out.writeInt(blocksRemaining);
      out.writeInt(errors);
    }
  }

  static class CrcInfoCommand extends UpgradeCommand {
    Block block;
    
    public CrcInfoCommand() {
      super(DN_CMD_CRC_INFO, DN_CMD_VERSION, (short)0);      
      block = new Block();
    }
    
    public CrcInfoCommand(Block blk) {
      // We don't need status
      super(DN_CMD_CRC_INFO, DN_CMD_VERSION, (short)0);
      block = blk;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      block.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      block.write(out);
    }
  }
  
  static class CrcInfoCommandReply extends UpgradeCommand {
    BlockCrcInfo crcInfo;
    
    public CrcInfoCommandReply(){
      super(DN_CMD_CRC_INFO_REPLY, DN_CMD_VERSION, (short)0);      
      crcInfo = new BlockCrcInfo();
    }
    
    public CrcInfoCommandReply(BlockCrcInfo info) {
      super(DN_CMD_CRC_INFO_REPLY, DN_CMD_VERSION, (short)0);
      crcInfo = info;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      crcInfo.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      crcInfo.write(out);
    }
  }

  static class BlockCrcUpgradeStatusReport extends UpgradeStatusReport {

    String extraText = "";
    
    public BlockCrcUpgradeStatusReport() {
    }
    
    public BlockCrcUpgradeStatusReport(int version, short status,
                                       String extraText) {
      super(version, status, false);
      this.extraText = extraText;
    }
    
    @Override
    public String getStatusText(boolean details) {
      return super.getStatusText(details) + "\n\n" + extraText;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      extraText = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeString(out, extraText);
    }
  }
}

/**
 * This class checks all the blocks and upgrades any that do not have 
 * meta data associated with them.
 * 
 * Things to consider : 
 *   Should it rescan at the end of the loop?
 */
class BlockCrcUpgradeObjectDatanode extends UpgradeObjectDatanode {

  public static final Log LOG = 
    LogFactory.getLog("org.apache.hadoop.dfs.BlockCrcUpgradeObjectDatanode");

  DatanodeProtocol namenode;
  
  // stats
  int blocksPreviouslyUpgraded;
  int blocksToUpgrade;
  int blocksUpgraded;
  int errors;

  //This should be a config. set it to 5 otherwise.
  static final int poolSize = 5;
  
  List<UpgradeExecutor> completedList = new LinkedList<UpgradeExecutor>();

  /* this is set when the datanode misses the regular upgrade.
   * When this is set, it upgrades the block by reading metadata from
   * the other replicas.
   */
  boolean offlineUpgrade = false;
  boolean upgradeCompleted = false;
  
  boolean isOfflineUpgradeOn() {
    return offlineUpgrade;
  }
  
  // common upgrade interface:
  
  public int getVersion() {
    return BlockCrcUpgradeUtils.PRE_BLOCK_CRC_LAYOUT_VERSION;
  }

  /*
   * Start upgrade if it not already running. It sends status to
   * namenode even if an upgrade is already in progress.
   */
  public synchronized UpgradeCommand startUpgrade() throws IOException {

    if ( offlineUpgrade ) {
      //run doUpgrade here.
      doUpgrade();
    }
    
    return null; 
  }

  
  @Override
  public String getDescription() {
    return "Block CRC Upgrade at Datanode";
  }

  @Override
  public short getUpgradeStatus() {
    return (blocksToUpgrade == blocksUpgraded) ? 100 :
      (short) Math.floor(blocksUpgraded*100.0/blocksToUpgrade);
  }

  @Override
  public UpgradeCommand completeUpgrade() throws IOException {
    // return latest stats command.
    assert getUpgradeStatus() == 100;
    return new BlockCrcUpgradeUtils.
               DatanodeStatsCommand(getUpgradeStatus(),                                 
                                    getDatanode().dnRegistration,
                                    blocksPreviouslyUpgraded + blocksUpgraded,
                                    blocksToUpgrade-blocksUpgraded,
                                    errors); 
  }
  
  
  // see description for super.preUpgradeAction().
  @Override
  boolean preUpgradeAction(NamespaceInfo nsInfo) throws IOException {
    int nsUpgradeVersion = nsInfo.getDistributedUpgradeVersion();
    if(nsUpgradeVersion >= getVersion()) {
      return false; // Normal upgrade.
    }
    
    LOG.info("\n  This Datanode has missed a cluster wide Block CRC Upgrade." +
             "\n  Will perform an 'offline' upgrade of the blocks." +
             "\n  During this time, Datanode does not heartbeat.");
    
    DataNode dataNode = getDatanode();
    
    //Make sure namenode removes this node from the registered nodes
    try {
      // Should we add another error type? Right now only DISK_ERROR removes it
      // from node list. 
      dataNode.namenode.errorReport(dataNode.dnRegistration,
                                    DatanodeProtocol.NOTIFY, 
                                    "Performing an offline upgrade. " +
                                    "Will be back online once the ugprade " +
                                    "completes. Please see datanode logs.");
      
    } catch(IOException ignored) {}
    
    offlineUpgrade = true;
    return true;
  }

  public BlockCrcUpgradeObjectDatanode() {
  }
  
  class UpgradeExecutor implements Runnable {
    Block block;
    Throwable throwable;
    
    UpgradeExecutor( Block b ) {
      block = b;
    }
    public void run() {
      try {
        DNBlockUpgradeInfo blockInfo = new DNBlockUpgradeInfo();
        blockInfo.block = block;
        blockInfo.dataNode = getDatanode();
        blockInfo.namenode = namenode;
        blockInfo.offlineUpgrade = offlineUpgrade;
        BlockCrcUpgradeUtils.upgradeBlock( blockInfo );
      } catch ( Throwable t ) {
        throwable = t;
      }
      synchronized (completedList) {
        completedList.add( this );
        completedList.notify();
      }
    }
  }
  
  @Override
  void doUpgrade() throws IOException {
    doUpgradeInternal();
  }
  
  private void doUpgradeInternal() {
    
    if ( upgradeCompleted ) {
      assert offlineUpgrade : 
             ("Multiple calls to doUpgrade is expected only during " +
              "offline upgrade");
      return;
    }
    
    FSDataset dataset = (FSDataset) getDatanode().data;

    // Set up the retry policy so that each attempt waits for one minute.
    Configuration conf = new Configuration();
    // set rpc timeout to one minute.
    conf.set("ipc.client.timeout", "60000");
    
    RetryPolicy timeoutPolicy = 
       RetryPolicies.retryUpToMaximumCountWithFixedSleep(
               BlockCrcUpgradeUtils.LONG_TIMEOUT_MINUTES,
               1, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(SocketTimeoutException.class, timeoutPolicy);
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = 
                            new HashMap<String, RetryPolicy>();
    // do we need to set the policy for connection failures also? 
    methodNameToPolicyMap.put("processUpgradeCommand", methodPolicy);
    methodNameToPolicyMap.put("blockCrcUpgradeGetBlockLocations", methodPolicy);
    
    LOG.info("Starting Block CRC upgrade.");
    
    for (;;) {
      try {
        namenode = (DatanodeProtocol) RetryProxy.create(
                            DatanodeProtocol.class,
                            RPC.waitForProxy(DatanodeProtocol.class,
                                             DatanodeProtocol.versionID,
                                             getDatanode().getNameNodeAddr(),
                                             conf),
                            methodNameToPolicyMap);
        break;
      } catch (IOException e) {
        LOG.warn("Exception while trying to connect to NameNode at " +
                 getDatanode().getNameNodeAddr().toString() + " : " + 
                 StringUtils.stringifyException(e));
        BlockCrcUpgradeUtils.sleep(10, "will retry connecting to NameNode");
      }
    }
                                  
    conf = null;
   
    // Get a list of all the blocks :
   
    LinkedList<UpgradeExecutor> blockList = new LinkedList<UpgradeExecutor>();
    
    //Fill blockList with blocks to be upgraded.
    Block [] blockArr = dataset.getBlockReport();
    
    for ( Block b : blockArr ) {
      File blockFile = null;
      try {
        blockFile = dataset.getBlockFile( b );
      } catch (IOException e) {
        //The block might just be deleted. ignore it.
        LOG.warn("Could not find file location for " + b + 
                 ". It might already be deleted. Exception : " +
                 StringUtils.stringifyException(e));
        errors++;
        continue;
      }
      if (!blockFile.exists()) {
        LOG.error("could not find block file " + blockFile);
        errors++;
        continue;
      }
      File metaFile = FSDataset.getMetaFile( blockFile );
      if ( metaFile.exists() ) {
        blocksPreviouslyUpgraded++;
      } else {
        blocksToUpgrade++;
        blockList.add( new UpgradeExecutor( b ) );
      }
    }
    blockArr = null;
    
    ExecutorService pool = Executors.newFixedThreadPool( poolSize );
    
    LOG.info("Starting upgrade of " + blocksToUpgrade + " blocks out of " +
             (blocksToUpgrade + blocksPreviouslyUpgraded));
    
    // Do we need to do multiple retries for each upgrade?
    
    for (Iterator<UpgradeExecutor> it = blockList.iterator(); it.hasNext();) {
      pool.submit( it.next() );
    }

    // Inform the namenode
    sendStatus();
    
    // Now wait for the tasks to complete.
    int nLeft = blockList.size();
    
    long now = System.currentTimeMillis();
    // Report status to namenode every so many seconds:
    long statusReportIntervalMilliSec = 60*1000;
    long lastStatusReportTime = now;
    long lastUpdateTime = now;
    long lastWarnTime = now;
    
    while ( nLeft > 0 ) {
      synchronized ( completedList ) {
        if ( completedList.size() <= 0 ) {
          try {
            completedList.wait(1000);
          } catch (InterruptedException ignored) {}
        }
        
        now = System.currentTimeMillis();
        
        if ( completedList.size() > 0 ) {
          UpgradeExecutor exe = completedList.remove(0);
          nLeft--;
          if ( exe.throwable != null ) {
            errors++;
            LOG.error("Got an exception during upgrade of " +
                      exe.block + ": " + 
                      StringUtils.stringifyException( exe.throwable ));
          }
          blocksUpgraded++;
          lastUpdateTime = now;
        } else {
          if ((now - lastUpdateTime) >= 
                BlockCrcUpgradeUtils.LONG_TIMEOUT_MILLISEC &&
              (now - lastWarnTime) >= 
                BlockCrcUpgradeUtils.LONG_TIMEOUT_MILLISEC) {
            lastWarnTime = now;
            LOG.warn("No block was updated in last " +
                      BlockCrcUpgradeUtils.LONG_TIMEOUT_MINUTES +
                      " minutes! will keep waiting... ");
          }  
        } 
      }
      
      if ( (now-lastStatusReportTime) > statusReportIntervalMilliSec ) {
        sendStatus();
        lastStatusReportTime = System.currentTimeMillis();
      }
    }

    upgradeCompleted = true;
    
    LOG.info("Completed BlockCrcUpgrade. total of " + 
             (blocksPreviouslyUpgraded + blocksToUpgrade) +
             " blocks : " + blocksPreviouslyUpgraded + " blocks previously " +
             "upgraded, " + blocksUpgraded + " blocks upgraded this time " +
             "with " + errors + " errors.");       

    // now inform the name node about the completion.

    // What if there is no upgrade running on Namenode now?
    while (!sendStatus());
    
  }
  
  /** Sends current status and stats to namenode and logs it to local log*/ 
  boolean sendStatus() {
    BlockCrcUpgradeUtils.DatanodeStatsCommand cmd = null;
    
    LOG.info((offlineUpgrade ? "Offline " : "") + "Block CRC Upgrade : " + 
             getUpgradeStatus() + "% completed.");
    
    if (offlineUpgrade) {
      return true;
    }
    
    synchronized (this) {
      cmd = new BlockCrcUpgradeUtils.
      DatanodeStatsCommand(getUpgradeStatus(),                                 
                           getDatanode().dnRegistration,
                           blocksPreviouslyUpgraded + blocksUpgraded,
                           blocksToUpgrade-blocksUpgraded,
                           errors);
    }

    UpgradeCommand reply = BlockCrcUpgradeUtils.sendCommand(namenode, cmd, 0);
    if ( reply == null ) {
      LOG.warn("Could not send status to Namenode. Namenode might be " +
               "over loaded or down.");
    }

    // its ok even if reply == noUpgradeOnNamenode
    return reply != null;
  }
}

/**
 * Once an upgrade starts at the namenode , this class manages the upgrade 
 * process.
 */
class BlockCrcUpgradeObjectNamenode extends UpgradeObjectNamenode {
  
  public static final Log LOG = 
    LogFactory.getLog("org.apache.hadoop.dfs.BlockCrcUpgradeNamenode");
  
  static final long inactivityExtension = 10*1000; // 10 seconds
  long lastNodeCompletionTime = 0;
  
  enum UpgradeStatus {
    INITIALIZED,
    STARTED,
    DATANODES_DONE,
    COMPLETED,
  }
  
  UpgradeStatus upgradeStatus = UpgradeStatus.INITIALIZED;
  
  class DnInfo { 
    short percentCompleted = 0;
    long blocksUpgraded = 0;
    long blocksRemaining = 0;
    long errors = 0;
    
    DnInfo(short pcCompleted) {
      percentCompleted = status;
    }
    DnInfo() {}
    
    void setStats(BlockCrcUpgradeUtils.DatanodeStatsCommand cmd) {
      percentCompleted = cmd.getCurrentStatus();
      blocksUpgraded = cmd.blocksUpgraded;
      blocksRemaining = cmd.blocksRemaining;
      errors = cmd.errors;
    }
    
    boolean isDone() {
      return percentCompleted >= 100;
    }
  }
  
  /* We should track only the storageIDs and not DatanodeID, which
   * includes datanode name and storage id.
   */
  HashMap<DatanodeID, DnInfo> dnMap = new HashMap<DatanodeID, DnInfo>();
  HashMap<DatanodeID, DnInfo> unfinishedDnMap = 
                                      new HashMap<DatanodeID, DnInfo>();  

  HashMap<INodeMapEntry, INodeMapEntry> iNodeParentMap = null;
  
  Daemon monitorThread;
  double avgDatanodeCompletionPct = 0;
  
  boolean forceDnCompletion = false;
  
  //Upgrade object interface:
  
  public int getVersion() {
    return BlockCrcUpgradeUtils.PRE_BLOCK_CRC_LAYOUT_VERSION;
  }

  public UpgradeCommand completeUpgrade() throws IOException {
    return null;
  }
 
  @Override
  public String getDescription() {
    return "Block CRC Upgrade at Namenode"; 
  }

  @Override
  public synchronized short getUpgradeStatus() {
    // Reserve 10% for deleting files.
    if ( upgradeStatus == UpgradeStatus.COMPLETED ) {
      return 100;
    }   
    if ( upgradeStatus == UpgradeStatus.DATANODES_DONE ) {
      return 90;
    }
    
    return (short) Math.floor(avgDatanodeCompletionPct * 0.9);
  }

  @Override
  public UpgradeCommand startUpgrade() throws IOException {
    
    assert monitorThread == null;
    
    buildINodeToParentMap();
    
    lastNodeCompletionTime = System.currentTimeMillis();
    
    monitorThread = new Daemon(new UpgradeMonitor());
    monitorThread.start();    
    
    return super.startUpgrade();
  }
  
  @Override
  public synchronized void forceProceed() throws IOException {    
    if (isUpgradeDone() || 
        upgradeStatus == UpgradeStatus.DATANODES_DONE) {
      LOG.info("forceProceed is a no op now since the stage waiting " +
               "waiting for Datanode to completed is finished. " +
               "Upgrade should soon complete");
      return;
    }
    
    if (forceDnCompletion) {
      LOG.warn("forceProceed is already set for this upgrade. It can take " +
               "a short while to take affect. Please wait.");
      return;
    }
    
    LOG.info("got forceProceed request for this upgrade. Datanodes upgrade " +
             "will be considered done. It can take a few seconds to take " +
             "effect.");
    forceDnCompletion = true;
  }

  @Override
  UpgradeCommand processUpgradeCommand(UpgradeCommand command) 
                                           throws IOException {
    switch (command.getAction()) {

    case BlockCrcUpgradeUtils.DN_CMD_CRC_INFO :
      return handleCrcInfoCmd(command);
    
    case BlockCrcUpgradeUtils.DN_CMD_STATS :
      return handleStatsCmd(command);

     default:
       throw new IOException("Unknown Command for BlockCrcUpgrade : " +
                             command.getAction());
    }
  }

  @Override
  public UpgradeStatusReport getUpgradeStatusReport(boolean details) 
                                                    throws IOException {

    /* If 'details' is true should we update block level status?
     * It could take multiple minutes
     * updateBlckLevelStats()?
     */
    
    String replyString = "";
    
    short status = 0;
    
    synchronized (this) {
     
      status = getUpgradeStatus();
     
      replyString = String.format(
      ((monitorThread == null) ? "\tUpgrade has not been started yet.\n" : "")+
      ((forceDnCompletion) ? "\tForce Proceed is ON\n" : "") +
      "\tLast Block Level Stats updated at : %tc\n" +
      "\tLast Block Level Stats : %s\n" +
      "\tBrief Datanode Status  : %s\n" +
      "%s",
      latestBlockLevelStats.updatedAt,
      latestBlockLevelStats.statusString("\n\t                         "), 
      printStatus("\n\t                         "), 
      ((status < 100 && upgradeStatus == UpgradeStatus.DATANODES_DONE) ?
      "\tNOTE: Upgrade at the Datanodes has finished. Deleteing \".crc\" " +
      "files\n\tcan take longer than status implies.\n" : "")
      );
      
      if (details) {
        // list all the known data nodes
        StringBuilder str = null;
        Iterator<DatanodeID> keys = dnMap.keySet().iterator();
        Iterator<DnInfo> values = dnMap.values().iterator();
        
        for(; keys.hasNext() && values.hasNext() ;) {
          DatanodeID dn = keys.next();
          DnInfo info = values.next();
          String dnStr = "\t\t" + dn.getName() + "\t : " + 
                         info.percentCompleted + " % \t" +
                         info.blocksUpgraded + " u \t" +
                         info.blocksRemaining + " r \t" +
                         info.errors + " e\n";
          if ( str == null ) {
            str = new StringBuilder(dnStr.length()*
                                    (dnMap.size() + (dnMap.size()+7)/8));
          }
          str.append(dnStr);
        }
        
        replyString += "\n\tDatanode Stats (total: " + dnMap.size() + "): " +
                       "pct Completion(%) blocks upgraded (u) " +
                       "blocks remaining (r) errors (e)\n\n" +
                       (( str == null ) ?
                        "\t\tThere are no known Datanodes\n" : str);
      }      
    }
    
    return new BlockCrcUpgradeUtils.BlockCrcUpgradeStatusReport(
                   BlockCrcUpgradeUtils.PRE_BLOCK_CRC_LAYOUT_VERSION,
                   status, replyString);
  }

  private UpgradeCommand handleCrcInfoCmd(UpgradeCommand cmd) {
    BlockCrcUpgradeUtils.CrcInfoCommand crcCmd =
                   (BlockCrcUpgradeUtils.CrcInfoCommand)cmd;
    
    BlockCrcInfo crcInfo = getFSNamesystem().blockCrcInfo(crcCmd.block,
                                                          this,
                                                          false);
    return new BlockCrcUpgradeUtils.CrcInfoCommandReply(crcInfo);
  }
  
  private synchronized UpgradeCommand handleStatsCmd(UpgradeCommand cmd) {
    
    BlockCrcUpgradeUtils.DatanodeStatsCommand stats =
      (BlockCrcUpgradeUtils.DatanodeStatsCommand)cmd;
    
    DatanodeID dn = stats.datanodeId;
    DnInfo dnInfo = dnMap.get(dn);
    boolean alreadyCompleted = (dnInfo != null && dnInfo.isDone());
    
    if ( dnInfo == null ) {
      dnInfo = new DnInfo();
      dnMap.put(dn, dnInfo);
      LOG.info("Upgrade started/resumed at datanode " + dn.getName());  
    }
    
    dnInfo.setStats(stats);

    if ( !dnInfo.isDone() ) {
      unfinishedDnMap.put(dn, dnInfo);
    }
    
    if ( dnInfo.isDone() && !alreadyCompleted ) {
      LOG.info("upgrade completed on datanode " + dn.getName());      
      unfinishedDnMap.remove(dn);
      if (unfinishedDnMap.size() == 0) {
        lastNodeCompletionTime = System.currentTimeMillis();
      }
    }   
    
    //Should we send any more info?
    return new UpgradeCommand();
  }
  
  public BlockCrcUpgradeObjectNamenode() {
  }
  
  /* This is a wrapper class so that we can control equals() and hashCode().
   * INode's equals() and hashCode() are not suitable for INodeToParent
   * HashMap.
   */
  static class INodeMapEntry {
    INode iNode;
    INodeMapEntry parent;
    
    INodeMapEntry(INode iNode, INodeMapEntry parent) {
      this.iNode = iNode;
      this.parent = parent;
    }
    
    public int hashCode() {
      return System.identityHashCode(iNode);
    }
    public boolean equals(Object entry) {
      return entry instanceof INodeMapEntry &&
             ((INodeMapEntry)entry).iNode == iNode;
    }
    
    private StringBuilder getName() {
      StringBuilder str = (parent.parent == null) ? new StringBuilder() : 
                          parent.getName();
      str.append(Path.SEPARATOR);
      return str.append(iNode.getLocalName());
    }
    String getAbsoluteName() {
      return (parent == null) ? "/" : getName().toString();
    }
    
    INodeDirectory getParentINode() {
      return (parent == null) ? null : (INodeDirectory)parent.iNode;
    }
  }
  
  private INodeMapEntry addINodeParentEntry(INode inode, INodeMapEntry parent) {
    INodeMapEntry entry = new INodeMapEntry(inode, parent);
    iNodeParentMap.put(entry, entry);
    return entry;
  }

  private long addToINodeParentMap(INodeMapEntry parent) {
    long count = 0;
    INodeDirectory dir = ((INodeDirectory)parent.iNode);
    for(Iterator<INode> it = dir.getChildren().iterator(); it.hasNext();) {
      INode inode = it.next();
      if ( inode.isDirectory() ) {
        count += 1 + addToINodeParentMap( addINodeParentEntry(inode, parent) );
      } else {
        // add only files that have associated ".crc" files.
        if ( dir.getChild("." + inode.getLocalName() + ".crc") != null ) {
          addINodeParentEntry(inode, parent);
          count++;
        }
      }
    }
    return count;
  }
  
  INodeMapEntry getINodeMapEntry(INode iNode) {
    return iNodeParentMap.get(new INodeMapEntry(iNode, null));
  }
  
  // builds INode to parent map for non ".crc" files.
  private void buildINodeToParentMap() {
    //larger intitial value should be ok for small clusters also.
    iNodeParentMap = new HashMap<INodeMapEntry, INodeMapEntry>(256*1024);
    
    LOG.info("Building INode to parent map.");
    
    //Iterate over the whole INode tree.
    INodeDirectory dir = getFSNamesystem().dir.rootDir;
    long numAdded = 1 + addToINodeParentMap(addINodeParentEntry(dir, null));
    
    LOG.info("Added " + numAdded + " entries to INode to parent map.");
  }
  
  // For now we will wait for all the nodes to complete upgrade.
  synchronized boolean isUpgradeDone() {
    return upgradeStatus == UpgradeStatus.COMPLETED;    
  }
  
  synchronized String printStatus(String spacing) {
    //NOTE: iterates on all the datanodes.
    
    // Calculate % completion on all the data nodes.
    long errors = 0;
    long totalCompletion = 0;
    for( Iterator<DnInfo> it = dnMap.values().iterator(); it.hasNext(); ) {
      DnInfo dnInfo = it.next();
      totalCompletion += dnInfo.percentCompleted;            
      errors += dnInfo.errors;
    }
    
    avgDatanodeCompletionPct = totalCompletion/(dnMap.size() + 1e-20);
    
    String msg = "Avg completion of all Datanodes: " +              
                 String.format("%.2f%%", avgDatanodeCompletionPct) +
                 " with " + errors + " errors. " +
                 ((unfinishedDnMap.size() > 0) ? spacing + 
                   unfinishedDnMap.size() + " out of " + dnMap.size() +
                   " nodes are not done." : "");
                 
    LOG.info("Block CRC Upgrade is " + (isUpgradeDone() ? 
             "complete. " : "still running. ") + spacing + msg);
    return msg;
  }
  
  private synchronized void setStatus(UpgradeStatus status) {
    upgradeStatus = status;
  }

  /* Checks if upgrade completed based on datanode's status and/or 
   * if all the blocks are upgraded.
   */
  private synchronized UpgradeStatus checkOverallCompletion() {
    
    if (upgradeStatus == UpgradeStatus.COMPLETED ||
        upgradeStatus == UpgradeStatus.DATANODES_DONE) {
      return upgradeStatus;
    }
    
    if (upgradeStatus != UpgradeStatus.DATANODES_DONE) {
      boolean datanodesDone =
        (dnMap.size() > 0 && unfinishedDnMap.size() == 0 &&
         ( System.currentTimeMillis() - lastNodeCompletionTime ) > 
        inactivityExtension) || forceDnCompletion ;
                 
      if ( datanodesDone ) {
        LOG.info("Upgrade of DataNode blocks is complete. " +
                 ((forceDnCompletion) ? "(ForceDnCompletion is on.)" : ""));
        upgradeStatus = UpgradeStatus.DATANODES_DONE;
      }
    }
    
    if (upgradeStatus != UpgradeStatus.DATANODES_DONE &&
        latestBlockLevelStats.updatedAt > 0) {
      // check if last block report marked all
      if (latestBlockLevelStats.minimallyReplicatedBlocks == 0 &&
          latestBlockLevelStats.underReplicatedBlocks == 0) {
        
        LOG.info("Marking datanode upgrade complete since all the blocks are " +
                 "upgraded (even though some datanodes may not have " +
                 "reported completion. Block level stats :\n\t" +
                 latestBlockLevelStats.statusString("\n\t"));
        upgradeStatus = UpgradeStatus.DATANODES_DONE;
      }
    }
    
    return upgradeStatus;
  } 
    
  /**
   * This class monitors the upgrade progress and periodically prints 
   * status message to log.
   */
  class UpgradeMonitor implements Runnable {
    
    static final long statusReportIntervalMillis = 1*60*1000;
    static final long blockReportIntervalMillis = 5*60*1000;
    static final int sleepTimeSec = 1;
    
    public void run() {
      long lastReportTime = System.currentTimeMillis();
      long lastBlockReportTime = lastReportTime;
      
      while ( !isUpgradeDone() ) {
        UpgradeStatus status = checkOverallCompletion();
        
        if ( status == UpgradeStatus.DATANODES_DONE ) {
          deleteCrcFiles();
          setStatus(UpgradeStatus.COMPLETED);
        }
        
        long now = System.currentTimeMillis();
        
        
        if (now-lastBlockReportTime >= blockReportIntervalMillis) {
          updateBlockLevelStats();
          // Check if all the blocks have been upgraded.
          lastBlockReportTime = now;
        }
        
        if ((now - lastReportTime) >= statusReportIntervalMillis || 
            isUpgradeDone()) {
          printStatus("\n\t");
          lastReportTime = now;
        }
        
        BlockCrcUpgradeUtils.sleep(sleepTimeSec, null);
      }
      
      LOG.info("Leaving the monitor thread");
    }
  }
  
  private BlockLevelStats latestBlockLevelStats = new BlockLevelStats();
  // internal class to hold the stats.
  private static class BlockLevelStats {
    long fullyReplicatedBlocks = 0;
    long minimallyReplicatedBlocks = 0;
    long underReplicatedBlocks = 0; // includes unReplicatedBlocks
    long unReplicatedBlocks = 0; // zero replicas upgraded
    long errors;
    long updatedAt;
    
    String statusString(String spacing) {
      long totalBlocks = fullyReplicatedBlocks + 
                         minimallyReplicatedBlocks +
                         underReplicatedBlocks;
      double multiplier = 100/(totalBlocks + 1e-20);
      
      if (spacing.equals("")) {
        spacing = ", ";
      }
      
      return String.format(
                     "Total Blocks : %d" +
                     "%sFully Upgragraded : %.2f%%" +
                     "%sMinimally Upgraded : %.2f%%" +
                     "%sUnder Upgraded : %.2f%% (includes Un-upgraded blocks)" +
                     "%sUn-upgraded : %.2f%%" + 
                     "%sErrors : %d", totalBlocks, 
                     spacing, (fullyReplicatedBlocks * multiplier),
                     spacing, (minimallyReplicatedBlocks * multiplier),
                     spacing, (underReplicatedBlocks * multiplier),
                     spacing, (unReplicatedBlocks * multiplier),
                     spacing, errors);
    }
  }
  
  void updateBlockLevelStats(String path, BlockLevelStats stats) {
    DFSFileInfo[] fileArr = getFSNamesystem().dir.getListing(path);
    
    for (DFSFileInfo file:fileArr) {
      if (file.isDir()) {
        updateBlockLevelStats(file.getPath().toString(), stats);
      } else {
        // Get the all the blocks.
        LocatedBlocks blockLoc = null;
        try {
          blockLoc = getFSNamesystem().getBlockLocations(
              file.getPath().toString(), 0, file.getLen());
          int numBlocks = blockLoc.locatedBlockCount();
          for (int i=0; i<numBlocks; i++) {
            LocatedBlock loc = blockLoc.get(i);
            DatanodeInfo[] dnArr = loc.getLocations();
            int numUpgraded = 0;
            synchronized (this) {
              for (DatanodeInfo dn:dnArr) {
                DnInfo dnInfo = dnMap.get(dn);
                if (dnInfo != null && dnInfo.isDone()) {
                  numUpgraded++;
                }
              }
            }
            
            if (numUpgraded >= file.getReplication()) {
              stats.fullyReplicatedBlocks++;
            } else if (numUpgraded >= getFSNamesystem().getMinReplication()) {
              stats.minimallyReplicatedBlocks++;
            } else {
              stats.underReplicatedBlocks++;
            }
            if (numUpgraded == 0) {
              stats.unReplicatedBlocks++;
            }
          }
        } catch (IOException e) {
          LOG.error("BlockCrcUpgrade: could not get block locations for " +
                    file.getPath().toString() + " : " +
                    StringUtils.stringifyException(e));
          stats.errors++;
        }
      }
    }
  }
  
  void updateBlockLevelStats() {
    /* This iterates over all the blocks and updates various 
     * counts.
     * Since iterating over all the blocks at once would be quite 
     * large operation under lock, we iterate over all the files
     * and update the counts for blocks that belong to a file.
     */
      
    LOG.info("Starting update of block level stats. " +
             "This could take a few minutes");
    BlockLevelStats stats = new BlockLevelStats();
    updateBlockLevelStats("/", stats);
    stats.updatedAt = System.currentTimeMillis();
    
    LOG.info("Block level stats:\n\t" + stats.statusString("\n\t"));
    synchronized (this) {
      latestBlockLevelStats = stats;
    }
  }
  
  private int deleteCrcFiles(String path) {
    // Recursively deletes files
    DFSFileInfo[] fileArr = getFSNamesystem().dir.getListing(path);
    
    int numFilesDeleted = 0;
    
    HashSet<String> fileSet = new HashSet<String>();
    
    // build a small hashMap
    for ( DFSFileInfo file:fileArr ) {
      String name = file.getName();
      if (!file.isDir() && (!name.startsWith(".") || 
                            !name.endsWith(".crc"))) {
        fileSet.add(name);
      }
    }
    
    for ( DFSFileInfo file:fileArr ) {
      if ( !file.isDir() ) {
        String name = file.getName();
        int extraLen = ".".length() + ".crc".length();
        if (name.startsWith(".") && name.endsWith(".crc") && 
            name.length() > extraLen) {
          String dataFile = name.substring(1, name.length()-extraLen+1);
          
          /* Deleting many files at once might be too much load on namenode,
           * especially on large clusters. We could throttle based on
           * some namesystem state. We can set high-low watermarks for 
           * pending deletes, etc.
           */
          if (fileSet.contains(dataFile)) {
            String filepath = path + (path.endsWith("/") ? "" : "/") + name;
            
            try {
              LOG.debug("Deleting " + filepath);
              if (getFSNamesystem().deleteInSafeMode(filepath)) {
                numFilesDeleted++;
              }
            } catch (IOException e) {
              LOG.error("Exception while deleting " + filepath + 
                        " : " + StringUtils.stringifyException(e));
            }
          }
        }
      }
    }
    
    // Reduce memory before recursion
    fileSet = null;
    
    for ( DFSFileInfo file:fileArr ) {
      if ( file.isDir() ) {
        numFilesDeleted += deleteCrcFiles(file.getPath().toString());
      }
    }
    
    return numFilesDeleted;
  }
  
  int deleteCrcFiles() {
    /* Iterate over all the files and delete any file with name .fname.crc
     * if fname exists in the directory.
     */
    while (true) {
      LOG.info("Deleting \".crc\" files. This may take a few minutes ... ");
      int numFilesDeleted = deleteCrcFiles("/");
      LOG.info("Deleted " + numFilesDeleted + " \".crc\" files");
      break;
      // Should we iterate again? No need for now!
    }
    return 0;
  } 
}
