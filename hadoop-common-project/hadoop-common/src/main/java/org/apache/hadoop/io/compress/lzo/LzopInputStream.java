package org.apache.hadoop.io.compress.lzo;

import io.airlift.compress.lzo.LzoDecompressor;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.hadoop.io.compress.CompressionInputStream;

import static java.lang.String.format;

public class LzopInputStream extends CompressionInputStream {
  static final int SIZE_OF_LONG = 8;
  private static final int LZO_VERSION_MAX = 0x20A0;
  private static final int LZOP_FILE_VERSION_MIN = 0x0940;
  private static final int LZOP_FORMAT_VERSION_MAX = 0x1010;

  private static final int LZOP_FLAG_ADLER32_DECOMPRESSED = 0x0000_0001;
  private static final int LZOP_FLAG_ADLER32_COMPRESSED = 0x0000_0002;
  private static final int LZOP_FLAG_CRC32_DECOMPRESSED = 0x0000_0100;
  private static final int LZOP_FLAG_CRC32_COMPRESSED = 0x0000_0200;
  private static final int LZOP_FLAG_CRC32_HEADER = 0x0000_1000;
  private static final int LZOP_FLAG_IO_MASK = 0x0000_000c;
  private static final int LZOP_FLAG_OPERATING_SYSTEM_MASK = 0xff00_0000;
  private static final int LZOP_FLAG_CHARACTER_SET_MASK = 0x00f0_0000;

  private final io.airlift.compress.lzo.LzoDecompressor decompressor = new LzoDecompressor();
  private final InputStream in;
  private final byte[] uncompressedChunk;

  private int uncompressedLength;
  private int uncompressedOffset;

  private boolean finished;

  private byte[] compressed = new byte[0];
  private final boolean adler32Decompressed;
  private final boolean adler32Compressed;
  private final boolean crc32Decompressed;
  private final boolean crc32Compressed;
  static final byte[] LZOP_MAGIC = new byte[] {(byte) 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};
  static final byte LZO_1X_VARIANT = 1;

  public LzopInputStream(InputStream in, int maxUncompressedLength)
          throws IOException
  {
    super(in);
    this.in = in;
    // over allocate buffer which makes decompression easier
    uncompressedChunk = new byte[maxUncompressedLength + SIZE_OF_LONG];

    byte[] magic = new byte[LZOP_MAGIC.length];
    readInput(magic, 0, magic.length);
    if (!Arrays.equals(magic, LZOP_MAGIC)) {
      throw new IOException("Not an LZOP file");
    }

    byte[] header = new byte[25];
    readInput(header, 0, header.length);
    ByteArrayInputStream headerStream = new ByteArrayInputStream(header);

    // lzop version: ignored
    int lzopFileVersion = readBigEndianShort(headerStream);
    if (lzopFileVersion < LZOP_FILE_VERSION_MIN) {
      throw new IOException(format("Unsupported LZOP file version 0x%08X", lzopFileVersion));
    }

    // lzo version
    int lzoVersion = readBigEndianShort(headerStream);
    if (lzoVersion > LZO_VERSION_MAX) {
      throw new IOException(format("Unsupported LZO version 0x%08X", lzoVersion));
    }

    // lzop version of the format
    int lzopFormatVersion = readBigEndianShort(headerStream);
    if (lzopFormatVersion > LZOP_FORMAT_VERSION_MAX) {
      throw new IOException(format("Unsupported LZOP format version 0x%08X", lzopFormatVersion));
    }

    // variant: must be LZO 1X
    int variant = headerStream.read();
    if (variant != LZO_1X_VARIANT) {
      throw new IOException(format("Unsupported LZO variant %s", variant));
    }

    // level: ignored
    headerStream.read();

    // flags
    int flags = readBigEndianInt(headerStream);

    // ignore flags about the compression environment
    flags &= ~LZOP_FLAG_IO_MASK;
    flags &= ~LZOP_FLAG_OPERATING_SYSTEM_MASK;
    flags &= ~LZOP_FLAG_CHARACTER_SET_MASK;

    // checksum flags
    adler32Decompressed = (flags & LZOP_FLAG_ADLER32_DECOMPRESSED) != 0;
    adler32Compressed = (flags & LZOP_FLAG_ADLER32_COMPRESSED) != 0;
    crc32Decompressed = (flags & LZOP_FLAG_CRC32_DECOMPRESSED) != 0;
    crc32Compressed = (flags & LZOP_FLAG_CRC32_COMPRESSED) != 0;
    boolean crc32Header = (flags & LZOP_FLAG_CRC32_HEADER) != 0;

    flags &= ~LZOP_FLAG_ADLER32_DECOMPRESSED;
    flags &= ~LZOP_FLAG_ADLER32_COMPRESSED;
    flags &= ~LZOP_FLAG_CRC32_DECOMPRESSED;
    flags &= ~LZOP_FLAG_CRC32_COMPRESSED;
    flags &= ~LZOP_FLAG_CRC32_HEADER;

    // no other flags are supported
    if (flags != 0) {
      throw new IOException(format("Unsupported LZO flags 0x%08X", flags));
    }

    // output file mode: ignored
    readBigEndianInt(headerStream);

    // output file modified time: ignored
    readBigEndianInt(headerStream);

    // output file time zone offset: ignored
    readBigEndianInt(headerStream);

    // output file name: ignored
    int fileNameLength = headerStream.read();
    byte[] fileName = new byte[fileNameLength];
    readInput(fileName, 0, fileName.length);

    // verify header checksum
    int headerChecksumValue = readBigEndianInt(in);

    Checksum headerChecksum = crc32Header ? new CRC32() : new Adler32();
    headerChecksum.update(header, 0, header.length);
    headerChecksum.update(fileName, 0, fileName.length);
    if (headerChecksumValue != (int) headerChecksum.getValue()) {
      throw new IOException("Invalid header checksum");
    }
  }

  @Override
  public int read()
          throws IOException
  {
    if (finished) {
      return -1;
    }

    while (uncompressedOffset >= uncompressedLength) {
      int compressedLength = bufferCompressedData();
      if (finished) {
        return -1;
      }

      decompress(compressedLength, uncompressedChunk, 0, uncompressedChunk.length);
    }
    return uncompressedChunk[uncompressedOffset++] & 0xFF;
  }

  @Override
  public int read(byte[] output, int offset, int length)
          throws IOException
  {
    if (finished) {
      return -1;
    }

    while (uncompressedOffset >= uncompressedLength) {
      int compressedLength = bufferCompressedData();
      if (finished) {
        return -1;
      }

      // favor writing directly to user buffer to avoid extra copy
      if (length >= uncompressedLength) {
        decompress(compressedLength, output, offset, length);
        uncompressedOffset = uncompressedLength;
        return uncompressedLength;
      }

      decompress(compressedLength, uncompressedChunk, 0, uncompressedChunk.length);
    }
    int size = Math.min(length, uncompressedLength - uncompressedOffset);
    System.arraycopy(uncompressedChunk, uncompressedOffset, output, offset, size);
    uncompressedOffset += size;
    return size;
  }

  @Override
  public void resetState()
          throws IOException
  {
    uncompressedLength = 0;
    uncompressedOffset = 0;
    finished = false;
  }

  private int bufferCompressedData()
          throws IOException
  {
    uncompressedOffset = 0;
    uncompressedLength = readBigEndianInt(in);
    if (uncompressedLength == -1) {
      // LZOP file MUST end with uncompressedLength == 0
      throw new EOFException("encountered EOF while reading block data");
    }
    if (uncompressedLength == 0) {
      finished = true;
      return -1;
    }

    int compressedLength = readBigEndianInt(in);
    if (compressedLength == -1) {
      throw new EOFException("encountered EOF while reading block data");
    }

    skipChecksums(compressedLength < uncompressedLength);

    return compressedLength;
  }

  private void skipChecksums(boolean compressed)
          throws IOException
  {
    if (adler32Decompressed) {
      readBigEndianInt(in);
    }
    if (crc32Decompressed) {
      readBigEndianInt(in);
    }
    if (compressed && adler32Compressed) {
      readBigEndianInt(in);
    }
    if (compressed && crc32Compressed) {
      readBigEndianInt(in);
    }
  }

  private void decompress(int compressedLength, byte[] output, int outputOffset, int outputLength)
          throws IOException
  {
    if (uncompressedLength == compressedLength) {
      readInput(output, outputOffset, compressedLength);
    }
    else {
      if (compressed.length < compressedLength) {
        // over allocate buffer which makes decompression easier
        compressed = new byte[compressedLength + SIZE_OF_LONG];
      }
      readInput(compressed, 0, compressedLength);
      int actualUncompressedLength = decompressor.decompress(compressed, 0, compressedLength, output, outputOffset, outputLength);
      if (actualUncompressedLength != uncompressedLength) {
        throw new IOException("Decompressor did not decompress the entire block");
      }
    }
  }

  private void readInput(byte[] buffer, int offset, int length)
          throws IOException
  {
    while (length > 0) {
      int size = in.read(buffer, offset, length);
      if (size == -1) {
        throw new EOFException("encountered EOF while reading block data");
      }
      offset += size;
      length -= size;
    }
  }

  private static int readBigEndianShort(InputStream in)
          throws IOException
  {
    int b1 = in.read();
    if (b1 < 0) {
      return -1;
    }

    int b2 = in.read();
    // If second byte is negative, the stream it truncated
    if ((b2) < 0) {
      throw new IOException("Stream is truncated");
    }
    return (b1 << 8) + (b2);
  }

  private static int readBigEndianInt(InputStream in)
          throws IOException
  {
    int b1 = in.read();
    if (b1 < 0) {
      return -1;
    }
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();

    // If any of the other bits are negative, the stream it truncated
    if ((b2 | b3 | b4) < 0) {
      throw new IOException("Stream is truncated");
    }
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4));
  }
}
