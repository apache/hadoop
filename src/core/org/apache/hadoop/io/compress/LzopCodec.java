/*
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

package org.apache.hadoop.io.compress;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.lzo.*;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} for a streaming
 * <b>lzo</b> compression/decompression pair compatible with lzop.
 * http://www.lzop.org/
 */
public class LzopCodec extends LzoCodec {

  private static final Log LOG = LogFactory.getLog(LzopCodec.class.getName());
  /** 9 bytes at the top of every lzo file */
  private static final byte[] LZO_MAGIC = new byte[] {
    -119, 'L', 'Z', 'O', 0, '\r', '\n', '\032', '\n' };
  /** Version of lzop this emulates */
  private static final int LZOP_VERSION = 0x1010;
  /** Latest verion of lzop this should be compatible with */
  private static final int LZOP_COMPAT_VERSION = 0x0940;

  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    LzoCompressor.CompressionStrategy strategy =
      LzoCompressor.CompressionStrategy.valueOf(
          getConf().get("io.compression.codec.lzo.compressor",
            LzoCompressor.CompressionStrategy.LZO1X_1.name()));
    int bufferSize =
      getConf().getInt("io.compression.codec.lzo.buffersize", 64*1024);
    return new LzopOutputStream(out, compressor, bufferSize, strategy);
  }

  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    return new LzopInputStream(in, decompressor,
        getConf().getInt("io.compression.codec.lzo.buffersize", 256 * 1024));
  }

  public Decompressor createDecompressor() {
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    return new LzopDecompressor(getConf().getInt(
          "io.compression.codec.lzo.buffersize", 256 * 1024));
  }

  public String getDefaultExtension() {
    return ".lzo";
  }

  /**
   * Checksums on decompressed block data with header bitmask, Checksum class.
   */
  private enum DChecksum {
    F_ADLER32D(0x01, Adler32.class), F_CRC32D(0x100, CRC32.class);
    private int mask;
    private Class<? extends Checksum> clazz;
    DChecksum(int mask, Class<? extends Checksum> clazz) {
      this.mask = mask;
      this.clazz = clazz;
    }
    public int getHeaderMask() {
      return mask;
    }
    public Class<? extends Checksum> getChecksumClass() {
      return clazz;
    }
  }

  /**
   * Checksums on compressed block data with header bitmask, Checksum class.
   */
  private enum CChecksum {
    F_ADLER32C(0x02, Adler32.class), F_CRC32C(0x200, CRC32.class);
    private int mask;
    private Class<? extends Checksum> clazz;
    CChecksum(int mask, Class<? extends Checksum> clazz) {
      this.mask = mask;
      this.clazz = clazz;
    }
    public int getHeaderMask() {
      return mask;
    }
    public Class<? extends Checksum> getChecksumClass() {
      return clazz;
    }
  };

  protected static class LzopOutputStream extends BlockCompressorStream {

    /**
     * Write an lzop-compatible header to the OutputStream provided.
     */
    protected static void writeLzopHeader(OutputStream out,
        LzoCompressor.CompressionStrategy strategy) throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        dob.writeShort(LZOP_VERSION);
        dob.writeShort(LzoCompressor.LZO_LIBRARY_VERSION);
        dob.writeShort(LZOP_COMPAT_VERSION);
        switch (strategy) {
          case LZO1X_1:
            dob.writeByte(1);
            dob.writeByte(5);
            break;
          case LZO1X_15:
            dob.writeByte(2);
            dob.writeByte(1);
            break;
          case LZO1X_999:
            dob.writeByte(3);
            dob.writeByte(9);
            break;
          default:
            throw new IOException("Incompatible lzop strategy: " + strategy);
        }
        dob.writeInt(0);                                    // all flags 0
        dob.writeInt(0x81A4);                               // mode
        dob.writeInt((int)(System.currentTimeMillis() / 1000)); // mtime
        dob.writeInt(0);                                    // gmtdiff ignored
        dob.writeByte(0);                                   // no filename
        Adler32 headerChecksum = new Adler32();
        headerChecksum.update(dob.getData(), 0, dob.getLength());
        int hc = (int)headerChecksum.getValue();
        dob.writeInt(hc);
        out.write(LZO_MAGIC);
        out.write(dob.getData(), 0, dob.getLength());
      } finally {
        dob.close();
      }
    }

    public LzopOutputStream(OutputStream out, Compressor compressor,
        int bufferSize, LzoCompressor.CompressionStrategy strategy)
        throws IOException {
      super(out, compressor, bufferSize, strategy.name().contains("LZO1")
          ? (bufferSize >> 4) + 64 + 3
          : (bufferSize >> 3) + 128 + 3);
      writeLzopHeader(out, strategy);
    }

    /**
     * Close the underlying stream and write a null word to the output stream.
     */
    public void close() throws IOException {
      if (!closed) {
        finish();
        out.write(new byte[]{ 0, 0, 0, 0 });
        out.close();
        closed = true;
      }
    }

  }

  protected static class LzopInputStream extends BlockDecompressorStream {

    private EnumSet<DChecksum> dflags = EnumSet.allOf(DChecksum.class);
    private EnumSet<CChecksum> cflags = EnumSet.allOf(CChecksum.class);

    private final byte[] buf = new byte[9];
    private EnumMap<DChecksum,Integer> dcheck
      = new EnumMap<DChecksum,Integer>(DChecksum.class);
    private EnumMap<CChecksum,Integer> ccheck
      = new EnumMap<CChecksum,Integer>(CChecksum.class);

    public LzopInputStream(InputStream in, Decompressor decompressor,
        int bufferSize) throws IOException {
      super(in, decompressor, bufferSize);
      readHeader(in);
    }

    /**
     * Read len bytes into buf, st LSB of int returned is the last byte of the
     * first word read.
     */
    private static int readInt(InputStream in, byte[] buf, int len) 
        throws IOException {
      if (0 > in.read(buf, 0, len)) {
        throw new EOFException();
      }
      int ret = (0xFF & buf[0]) << 24;
      ret    |= (0xFF & buf[1]) << 16;
      ret    |= (0xFF & buf[2]) << 8;
      ret    |= (0xFF & buf[3]);
      return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }

    /**
     * Read bytes, update checksums, return first four bytes as an int, first
     * byte read in the MSB.
     */
    private static int readHeaderItem(InputStream in, byte[] buf, int len,
        Adler32 adler, CRC32 crc32) throws IOException {
      int ret = readInt(in, buf, len);
      adler.update(buf, 0, len);
      crc32.update(buf, 0, len);
      Arrays.fill(buf, (byte)0);
      return ret;
    }

    /**
     * Read and verify an lzo header, setting relevant block checksum options
     * and ignoring most everything else.
     */
    protected void readHeader(InputStream in) throws IOException {
      if (0 > in.read(buf, 0, 9)) {
        throw new EOFException();
      }
      if (!Arrays.equals(buf, LZO_MAGIC)) {
        throw new IOException("Invalid LZO header");
      }
      Arrays.fill(buf, (byte)0);
      Adler32 adler = new Adler32();
      CRC32 crc32 = new CRC32();
      int hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop version
      if (hitem > LZOP_VERSION) {
        LOG.debug("Compressed with later version of lzop: " +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LZOP_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzo library version
      if (hitem > LzoDecompressor.LZO_LIBRARY_VERSION) {
        throw new IOException("Compressed with incompatible lzo version: 0x" +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LzoDecompressor.LZO_LIBRARY_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop extract version
      if (hitem > LZOP_VERSION) {
        throw new IOException("Compressed with incompatible lzop version: 0x" +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LZOP_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 1, adler, crc32); // method
      if (hitem < 1 || hitem > 3) {
          throw new IOException("Invalid strategy: " +
              Integer.toHexString(hitem));
      }
      readHeaderItem(in, buf, 1, adler, crc32); // ignore level

      // flags
      hitem = readHeaderItem(in, buf, 4, adler, crc32);
      try {
        for (DChecksum f : dflags) {
          if (0 == (f.getHeaderMask() & hitem)) {
            dflags.remove(f);
          } else {
            dcheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
          }
        }
        for (CChecksum f : cflags) {
          if (0 == (f.getHeaderMask() & hitem)) {
            cflags.remove(f);
          } else {
            ccheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
          }
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Internal error", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Internal error", e);
      }
      ((LzopDecompressor)decompressor).initHeaderFlags(dflags, cflags);
      boolean useCRC32 = 0 != (hitem & 0x00001000);   // F_H_CRC32
      boolean extraField = 0 != (hitem & 0x00000040); // F_H_EXTRA_FIELD
      if (0 != (hitem & 0x400)) {                     // F_MULTIPART
        throw new IOException("Multipart lzop not supported");
      }
      if (0 != (hitem & 0x800)) {                     // F_H_FILTER
        throw new IOException("lzop filter not supported");
      }
      if (0 != (hitem & 0x000FC000)) {                // F_RESERVED
        throw new IOException("Unknown flags in header");
      }
      // known !F_H_FILTER, so no optional block

      readHeaderItem(in, buf, 4, adler, crc32); // ignore mode
      readHeaderItem(in, buf, 4, adler, crc32); // ignore mtime
      readHeaderItem(in, buf, 4, adler, crc32); // ignore gmtdiff
      hitem = readHeaderItem(in, buf, 1, adler, crc32); // fn len
      if (hitem > 0) {
        // skip filename
        readHeaderItem(in, new byte[hitem], hitem, adler, crc32);
      }
      int checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
      hitem = readHeaderItem(in, buf, 4, adler, crc32); // read checksum
      if (hitem != checksum) {
        throw new IOException("Invalid header checksum: " +
            Long.toHexString(checksum) + " (expected 0x" +
            Integer.toHexString(hitem) + ")");
      }
      if (extraField) { // lzop 1.08 ultimately ignores this
        LOG.debug("Extra header field not processed");
        adler.reset();
        crc32.reset();
        hitem = readHeaderItem(in, buf, 4, adler, crc32);
        readHeaderItem(in, new byte[hitem], hitem, adler, crc32);
        checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
        if (checksum != readHeaderItem(in, buf, 4, adler, crc32)) {
          throw new IOException("Invalid checksum for extra header field");
        }
      }
    }

    /**
     * Take checksums recorded from block header and verify them against
     * those recorded by the decomrpessor.
     */
    private void verifyChecksums() throws IOException {
      LzopDecompressor ldecompressor = ((LzopDecompressor)decompressor);
      for (Map.Entry<DChecksum,Integer> chk : dcheck.entrySet()) {
        if (!ldecompressor.verifyDChecksum(chk.getKey(), chk.getValue())) {
          throw new IOException("Corrupted uncompressed block");
        }
      }
      for (Map.Entry<CChecksum,Integer> chk : ccheck.entrySet()) {
        if (!ldecompressor.verifyCChecksum(chk.getKey(), chk.getValue())) {
          throw new IOException("Corrupted compressed block");
        }
      }
    }

    /**
     * Read checksums and feed compressed block data into decompressor.
     */
    void getCompressedData() throws IOException {
      checkStream();

      LzopDecompressor ldecompressor = (LzopDecompressor)decompressor;

      // Get the size of the compressed chunk
      int len = readInt(in, buf, 4);

      verifyChecksums();

      for (DChecksum chk : dcheck.keySet()) {
        dcheck.put(chk, readInt(in, buf, 4));
      }
      for (CChecksum chk : ccheck.keySet()) {
        // NOTE: if the compressed size is not less than the uncompressed
        //       size, this value is not present and decompression will fail.
        //       Fortunately, checksums on compressed data are rare, as is
        //       this case.
        ccheck.put(chk, readInt(in, buf, 4));
      }

      ldecompressor.resetChecksum();

      // Read len bytes from underlying stream
      if (len > buffer.length) {
        buffer = new byte[len];
      }
      int n = 0, off = 0;
      while (n < len) {
        int count = in.read(buffer, off + n, len - n);
        if (count < 0) {
          throw new EOFException();
        }
        n += count;
      }

      // Send the read data to the decompressor
      decompressor.setInput(buffer, 0, len);
    }

    public void close() throws IOException {
      super.close();
      verifyChecksums();
    }
  }

  protected static class LzopDecompressor extends LzoDecompressor {

    private EnumMap<DChecksum,Checksum> chkDMap =
      new EnumMap<DChecksum,Checksum>(DChecksum.class);
    private EnumMap<CChecksum,Checksum> chkCMap =
      new EnumMap<CChecksum,Checksum>(CChecksum.class);
    private final int bufferSize;

    /**
     * Create an LzoDecompressor with LZO1X strategy (the only lzo algorithm
     * supported by lzop).
     */
    public LzopDecompressor(int bufferSize) {
      super(LzoDecompressor.CompressionStrategy.LZO1X_SAFE, bufferSize);
      this.bufferSize = bufferSize;
    }

    /**
     * Given a set of decompressed and compressed checksums, 
     */
    public void initHeaderFlags(EnumSet<DChecksum> dflags,
        EnumSet<CChecksum> cflags) {
      try {
        for (DChecksum flag : dflags) {
          chkDMap.put(flag, flag.getChecksumClass().newInstance());
        }
        for (CChecksum flag : cflags) {
          chkCMap.put(flag, flag.getChecksumClass().newInstance());
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Internal error", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Internal error", e);
      }
    }

    /**
     * Reset all checksums registered for this decompressor instance.
     */
    public synchronized void resetChecksum() {
      for (Checksum chk : chkDMap.values()) chk.reset();
      for (Checksum chk : chkCMap.values()) chk.reset();
    }

    /**
     * Given a checksum type, verify its value against that observed in
     * decompressed data.
     */
    public synchronized boolean verifyDChecksum(DChecksum typ, int checksum) {
      return (checksum == (int)chkDMap.get(typ).getValue());
    }

    /**
     * Given a checksum type, verity its value against that observed in
     * compressed data.
     */
    public synchronized boolean verifyCChecksum(CChecksum typ, int checksum) {
      return (checksum == (int)chkCMap.get(typ).getValue());
    }

    public synchronized void setInput(byte[] b, int off, int len) {
      for (Checksum chk : chkCMap.values()) chk.update(b, off, len);
      super.setInput(b, off, len);
    }

    public synchronized int decompress(byte[] b, int off, int len)
        throws IOException {
      int ret = super.decompress(b, off, len);
      if (ret > 0) {
        for (Checksum chk : chkDMap.values()) chk.update(b, off, ret);
      }
      return ret;
    }
  }

}
