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

package org.apache.hadoop.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;

/**
 * This class provides interface and utilities for processing checksums for
 * DFS data transfers.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DataChecksum implements Checksum {
  
  // checksum types
  public static final int CHECKSUM_NULL    = 0;
  public static final int CHECKSUM_CRC32   = 1;
  public static final int CHECKSUM_CRC32C  = 2;
  public static final int CHECKSUM_DEFAULT = 3; 
  public static final int CHECKSUM_MIXED   = 4;
 
  /** The checksum types */
  public static enum Type {
    NULL  (CHECKSUM_NULL, 0),
    CRC32 (CHECKSUM_CRC32, 4),
    CRC32C(CHECKSUM_CRC32C, 4),
    DEFAULT(CHECKSUM_DEFAULT, 0), // This cannot be used to create DataChecksum
    MIXED (CHECKSUM_MIXED, 0); // This cannot be used to create DataChecksum

    public final int id;
    public final int size;
    
    private Type(int id, int size) {
      this.id = id;
      this.size = size;
    }

    /** @return the type corresponding to the id. */
    public static Type valueOf(int id) {
      if (id < 0 || id >= values().length) {
        throw new IllegalArgumentException("id=" + id
            + " out of range [0, " + values().length + ")");
      }
      return values()[id];
    }
  }

  /**
   * Create a Crc32 Checksum object. The implementation of the Crc32 algorithm
   * is chosen depending on the platform.
   */
  public static Checksum newCrc32() {
    return Shell.isJava7OrAbove()? new CRC32(): new PureJavaCrc32();
  }

  public static DataChecksum newDataChecksum(Type type, int bytesPerChecksum ) {
    if ( bytesPerChecksum <= 0 ) {
      return null;
    }
    
    switch ( type ) {
    case NULL :
      return new DataChecksum(type, new ChecksumNull(), bytesPerChecksum );
    case CRC32 :
      return new DataChecksum(type, newCrc32(), bytesPerChecksum );
    case CRC32C:
      return new DataChecksum(type, new PureJavaCrc32C(), bytesPerChecksum);
    default:
      return null;  
    }
  }
  
  /**
   * Creates a DataChecksum from HEADER_LEN bytes from arr[offset].
   * @return DataChecksum of the type in the array or null in case of an error.
   */
  public static DataChecksum newDataChecksum( byte bytes[], int offset ) {
    if (offset < 0 || bytes.length < offset + getChecksumHeaderSize()) {
      return null;
    }
    
    // like readInt():
    int bytesPerChecksum = ( (bytes[offset+1] & 0xff) << 24 ) | 
                           ( (bytes[offset+2] & 0xff) << 16 ) |
                           ( (bytes[offset+3] & 0xff) << 8 )  |
                           ( (bytes[offset+4] & 0xff) );
    return newDataChecksum( Type.valueOf(bytes[offset]), bytesPerChecksum );
  }
  
  /**
   * This constructs a DataChecksum by reading HEADER_LEN bytes from input
   * stream <i>in</i>
   */
  public static DataChecksum newDataChecksum( DataInputStream in )
                                 throws IOException {
    int type = in.readByte();
    int bpc = in.readInt();
    DataChecksum summer = newDataChecksum(Type.valueOf(type), bpc );
    if ( summer == null ) {
      throw new IOException( "Could not create DataChecksum of type " +
                             type + " with bytesPerChecksum " + bpc );
    }
    return summer;
  }
  
  /**
   * Writes the checksum header to the output stream <i>out</i>.
   */
  public void writeHeader( DataOutputStream out ) 
                           throws IOException { 
    out.writeByte( type.id );
    out.writeInt( bytesPerChecksum );
  }

  public byte[] getHeader() {
    byte[] header = new byte[getChecksumHeaderSize()];
    header[0] = (byte) (type.id & 0xff);
    // Writing in buffer just like DataOutput.WriteInt()
    header[1+0] = (byte) ((bytesPerChecksum >>> 24) & 0xff);
    header[1+1] = (byte) ((bytesPerChecksum >>> 16) & 0xff);
    header[1+2] = (byte) ((bytesPerChecksum >>> 8) & 0xff);
    header[1+3] = (byte) (bytesPerChecksum & 0xff);
    return header;
  }
  
  /**
   * Writes the current checksum to the stream.
   * If <i>reset</i> is true, then resets the checksum.
   * @return number of bytes written. Will be equal to getChecksumSize();
   */
   public int writeValue( DataOutputStream out, boolean reset )
                          throws IOException {
     if ( type.size <= 0 ) {
       return 0;
     }

     if ( type.size == 4 ) {
       out.writeInt( (int) summer.getValue() );
     } else {
       throw new IOException( "Unknown Checksum " + type );
     }
     
     if ( reset ) {
       reset();
     }
     
     return type.size;
   }
   
   /**
    * Writes the current checksum to a buffer.
    * If <i>reset</i> is true, then resets the checksum.
    * @return number of bytes written. Will be equal to getChecksumSize();
    */
    public int writeValue( byte[] buf, int offset, boolean reset )
                           throws IOException {
      if ( type.size <= 0 ) {
        return 0;
      }

      if ( type.size == 4 ) {
        int checksum = (int) summer.getValue();
        buf[offset+0] = (byte) ((checksum >>> 24) & 0xff);
        buf[offset+1] = (byte) ((checksum >>> 16) & 0xff);
        buf[offset+2] = (byte) ((checksum >>> 8) & 0xff);
        buf[offset+3] = (byte) (checksum & 0xff);
      } else {
        throw new IOException( "Unknown Checksum " + type );
      }
      
      if ( reset ) {
        reset();
      }
      
      return type.size;
    }
   
   /**
    * Compares the checksum located at buf[offset] with the current checksum.
    * @return true if the checksum matches and false otherwise.
    */
   public boolean compare( byte buf[], int offset ) {
     if ( type.size == 4 ) {
       int checksum = ( (buf[offset+0] & 0xff) << 24 ) | 
                      ( (buf[offset+1] & 0xff) << 16 ) |
                      ( (buf[offset+2] & 0xff) << 8 )  |
                      ( (buf[offset+3] & 0xff) );
       return checksum == (int) summer.getValue();
     }
     return type.size == 0;
   }
   
  private final Type type;
  private final Checksum summer;
  private final int bytesPerChecksum;
  private int inSum = 0;
  
  private DataChecksum( Type type, Checksum checksum, int chunkSize ) {
    this.type = type;
    summer = checksum;
    bytesPerChecksum = chunkSize;
  }
  
  /** @return the checksum algorithm type. */
  public Type getChecksumType() {
    return type;
  }
  /** @return the size for a checksum. */
  public int getChecksumSize() {
    return type.size;
  }
  /** @return the required checksum size given the data length. */
  public int getChecksumSize(int dataSize) {
    return ((dataSize - 1)/getBytesPerChecksum() + 1) * getChecksumSize(); 
  }
  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }
  public int getNumBytesInSum() {
    return inSum;
  }
  
  public static final int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;
  static public int getChecksumHeaderSize() {
    return 1 + SIZE_OF_INTEGER; // type byte, bytesPerChecksum int
  }
  //Checksum Interface. Just a wrapper around member summer.
  @Override
  public long getValue() {
    return summer.getValue();
  }
  @Override
  public void reset() {
    summer.reset();
    inSum = 0;
  }
  @Override
  public void update( byte[] b, int off, int len ) {
    if ( len > 0 ) {
      summer.update( b, off, len );
      inSum += len;
    }
  }
  @Override
  public void update( int b ) {
    summer.update( b );
    inSum += 1;
  }
  
  /**
   * Verify that the given checksums match the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,.
   * but the position is maintained.
   *  
   * @param data the DirectByteBuffer pointing to the data to verify.
   * @param checksums the DirectByteBuffer pointing to a series of stored
   *                  checksums
   * @param fileName the name of the file being read, for error-reporting
   * @param basePos the file position to which the start of 'data' corresponds
   * @throws ChecksumException if the checksums do not match
   */
  public void verifyChunkedSums(ByteBuffer data, ByteBuffer checksums,
      String fileName, long basePos)
  throws ChecksumException {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      verifyChunkedSums(
          data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position(),
          fileName, basePos);
      return;
    }
    if (NativeCrc32.isAvailable()) {
      NativeCrc32.verifyChunkedSums(bytesPerChecksum, type.id, checksums, data,
          fileName, basePos);
      return;
    }
    
    int startDataPos = data.position();
    data.mark();
    checksums.mark();
    try {
      byte[] buf = new byte[bytesPerChecksum];
      byte[] sum = new byte[type.size];
      while (data.remaining() > 0) {
        int n = Math.min(data.remaining(), bytesPerChecksum);
        checksums.get(sum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        int calculated = (int)summer.getValue();
        int stored = (sum[0] << 24 & 0xff000000) |
          (sum[1] << 16 & 0xff0000) |
          (sum[2] << 8 & 0xff00) |
          sum[3] & 0xff;
        if (calculated != stored) {
          long errPos = basePos + data.position() - startDataPos - n;
          throw new ChecksumException(
              "Checksum error: "+ fileName + " at "+ errPos +
              " exp: " + stored + " got: " + calculated, errPos);
        }
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }
  
  /**
   * Implementation of chunked verification specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  private void verifyChunkedSums(
      byte[] data, int dataOff, int dataLen,
      byte[] checksums, int checksumsOff, String fileName,
      long basePos) throws ChecksumException {
    if (type.size == 0) return;

    if (NativeCrc32.isAvailable()) {
      NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum, type.id,
          checksums, checksumsOff, data, dataOff, dataLen, fileName, basePos);
      return;
    }
    
    int remaining = dataLen;
    int dataPos = 0;
    while (remaining > 0) {
      int n = Math.min(remaining, bytesPerChecksum);
      
      summer.reset();
      summer.update(data, dataOff + dataPos, n);
      dataPos += n;
      remaining -= n;
      
      int calculated = (int)summer.getValue();
      int stored = (checksums[checksumsOff] << 24 & 0xff000000) |
        (checksums[checksumsOff + 1] << 16 & 0xff0000) |
        (checksums[checksumsOff + 2] << 8 & 0xff00) |
        checksums[checksumsOff + 3] & 0xff;
      checksumsOff += 4;
      if (calculated != stored) {
        long errPos = basePos + dataPos - n;
        throw new ChecksumException(
            "Checksum error: "+ fileName + " at "+ errPos +
            " exp: " + stored + " got: " + calculated, errPos);
      }
    }
  }

  /**
   * Calculate checksums for the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,
   * but the position is maintained.
   * 
   * @param data the DirectByteBuffer pointing to the data to checksum.
   * @param checksums the DirectByteBuffer into which checksums will be
   *                  stored. Enough space must be available in this
   *                  buffer to put the checksums.
   */
  public void calculateChunkedSums(ByteBuffer data, ByteBuffer checksums) {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      calculateChunkedSums(data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position());
      return;
    }

    if (NativeCrc32.isAvailable()) {
      NativeCrc32.calculateChunkedSums(bytesPerChecksum, type.id,
          checksums, data);
      return;
    }
    
    data.mark();
    checksums.mark();
    try {
      byte[] buf = new byte[bytesPerChecksum];
      while (data.remaining() > 0) {
        int n = Math.min(data.remaining(), bytesPerChecksum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        checksums.putInt((int)summer.getValue());
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }

  /**
   * Implementation of chunked calculation specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  public void calculateChunkedSums(
      byte[] data, int dataOffset, int dataLength,
      byte[] sums, int sumsOffset) {
    if (type.size == 0) return;

    if (NativeCrc32.isAvailable()) {
      NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum, type.id,
          sums, sumsOffset, data, dataOffset, dataLength);
      return;
    }

    int remaining = dataLength;
    while (remaining > 0) {
      int n = Math.min(remaining, bytesPerChecksum);
      summer.reset();
      summer.update(data, dataOffset, n);
      dataOffset += n;
      remaining -= n;
      long calculated = summer.getValue();
      sums[sumsOffset++] = (byte) (calculated >> 24);
      sums[sumsOffset++] = (byte) (calculated >> 16);
      sums[sumsOffset++] = (byte) (calculated >> 8);
      sums[sumsOffset++] = (byte) (calculated);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DataChecksum)) {
      return false;
    }
    DataChecksum o = (DataChecksum)other;
    return o.bytesPerChecksum == this.bytesPerChecksum &&
      o.type == this.type;
  }
  
  @Override
  public int hashCode() {
    return (this.type.id + 31) * this.bytesPerChecksum;
  }
  
  @Override
  public String toString() {
    return "DataChecksum(type=" + type +
      ", chunkSize=" + bytesPerChecksum + ")";
  }
  
  /**
   * This just provides a dummy implimentation for Checksum class
   * This is used when there is no checksum available or required for 
   * data
   */
  static class ChecksumNull implements Checksum {
    
    public ChecksumNull() {}
    
    //Dummy interface
    @Override
    public long getValue() { return 0; }
    @Override
    public void reset() {}
    @Override
    public void update(byte[] b, int off, int len) {}
    @Override
    public void update(int b) {}
  };
}
