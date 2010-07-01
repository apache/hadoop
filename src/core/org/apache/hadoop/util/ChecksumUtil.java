package org.apache.hadoop.util;

import java.io.IOException;

public class ChecksumUtil {
  /**
   * updates the checksum for a buffer
   * 
   * @param buf - buffer to update the checksum in
   * @param chunkOff - offset in the buffer where the checksum is to update
   * @param dataOff - offset in the buffer of the data
   * @param dataLen - length of data to compute checksum on
   */
  public static void updateChunkChecksum(
    byte[] buf,
    int checksumOff,
    int dataOff, 
    int dataLen,
    DataChecksum checksum
  ) throws IOException {
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    int checksumSize = checksum.getChecksumSize();
    int curChecksumOff = checksumOff;
    int curDataOff = dataOff;
    int numChunks = (dataLen + bytesPerChecksum - 1) / bytesPerChecksum;
    int dataLeft = dataLen;
    
    for (int i = 0; i < numChunks; i++) {
      int len = Math.min(dataLeft, bytesPerChecksum);
      
      checksum.reset();
      checksum.update(buf, curDataOff, len);
      checksum.writeValue(buf, curChecksumOff, false);
      
      curDataOff += len;
      curChecksumOff += checksumSize;
      dataLeft -= len;
    }
  }
}
