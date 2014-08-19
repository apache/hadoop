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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;



/**
 * BlockMetadataHeader manages metadata for data blocks on Datanodes.
 * This is not related to the Block related functionality in Namenode.
 * The biggest part of data block metadata is CRC for the block.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockMetadataHeader {

  public static final short VERSION = 1;
  
  /**
   * Header includes everything except the checksum(s) themselves.
   * Version is two bytes. Following it is the DataChecksum
   * that occupies 5 bytes. 
   */
  private final short version;
  private DataChecksum checksum = null;
    
  @VisibleForTesting
  public BlockMetadataHeader(short version, DataChecksum checksum) {
    this.checksum = checksum;
    this.version = version;
  }
  
  /** Get the version */
  public short getVersion() {
    return version;
  }

  /** Get the checksum */
  public DataChecksum getChecksum() {
    return checksum;
  }

  /**
   * Read the header without changing the position of the FileChannel.
   *
   * @param fc The FileChannel to read.
   * @return the Metadata Header.
   * @throws IOException on error.
   */
  public static BlockMetadataHeader preadHeader(FileChannel fc)
      throws IOException {
    byte arr[] = new byte[2 + DataChecksum.HEADER_LEN];
    ByteBuffer buf = ByteBuffer.wrap(arr);

    while (buf.hasRemaining()) {
      if (fc.read(buf, 0) <= 0) {
        throw new EOFException("unexpected EOF while reading " +
            "metadata file header");
      }
    }
    short version = (short)((arr[0] << 8) | (arr[1] & 0xff));
    DataChecksum dataChecksum = DataChecksum.newDataChecksum(arr, 2);
    return new BlockMetadataHeader(version, dataChecksum);
  }

  /**
   * This reads all the fields till the beginning of checksum.
   * @return Metadata Header
   * @throws IOException
   */
  public static BlockMetadataHeader readHeader(DataInputStream in) throws IOException {
    return readHeader(in.readShort(), in);
  }
  
  /**
   * Reads header at the top of metadata file and returns the header.
   * 
   * @return metadata header for the block
   * @throws IOException
   */
  public static BlockMetadataHeader readHeader(File file) throws IOException {
    DataInputStream in = null;
    try {
      in = new DataInputStream(new BufferedInputStream(
                               new FileInputStream(file)));
      return readHeader(in);
    } finally {
      IOUtils.closeStream(in);
    }
  }
  
  /**
   * Read the header at the beginning of the given block meta file.
   * The current file position will be altered by this method.
   * If an error occurs, the file is <em>not</em> closed.
   */
  static BlockMetadataHeader readHeader(RandomAccessFile raf) throws IOException {
    byte[] buf = new byte[getHeaderSize()];
    raf.seek(0);
    raf.readFully(buf, 0, buf.length);
    return readHeader(new DataInputStream(new ByteArrayInputStream(buf)));
  }
  
  // Version is already read.
  private static BlockMetadataHeader readHeader(short version, DataInputStream in) 
                                   throws IOException {
    DataChecksum checksum = DataChecksum.newDataChecksum(in);
    return new BlockMetadataHeader(version, checksum);
  }
  
  /**
   * This writes all the fields till the beginning of checksum.
   * @param out DataOutputStream
   * @throws IOException
   */
  @VisibleForTesting
  public static void writeHeader(DataOutputStream out, 
                                  BlockMetadataHeader header) 
                                  throws IOException {
    out.writeShort(header.getVersion());
    header.getChecksum().writeHeader(out);
  }
  
  /**
   * Writes all the fields till the beginning of checksum.
   * @throws IOException on error
   */
  static void writeHeader(DataOutputStream out, DataChecksum checksum)
                         throws IOException {
    writeHeader(out, new BlockMetadataHeader(VERSION, checksum));
  }

  /**
   * Returns the size of the header
   */
  public static int getHeaderSize() {
    return Short.SIZE/Byte.SIZE + DataChecksum.getChecksumHeaderSize();
  }
}

