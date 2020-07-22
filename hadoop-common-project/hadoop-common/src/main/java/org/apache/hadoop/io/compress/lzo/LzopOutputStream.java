package org.apache.hadoop.io.compress.lzo;

import io.airlift.compress.lzo.LzoCompressor;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class LzopOutputStream extends CompressionOutputStream
{
  static final int SIZE_OF_LONG = 8;
  private static final int LZOP_FILE_VERSION = 0x1010;
  private static final int LZOP_FORMAT_VERSION = 0x0940;
  private static final int LZO_FORMAT_VERSION = 0x2050;
  private static final int LEVEL = 5;
  static final byte[] LZOP_MAGIC = new byte[] {(byte) 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};
  static final byte LZO_1X_VARIANT = 1;

  private final io.airlift.compress.lzo.LzoCompressor compressor = new LzoCompressor();

  private final byte[] inputBuffer;
  private final int inputMaxSize;
  private int inputOffset;

  private final byte[] outputBuffer;

  public LzopOutputStream(OutputStream out, int bufferSize)
          throws IOException
  {
    super(out);
    inputBuffer = new byte[bufferSize];
    // leave extra space free at end of buffers to make compression (slightly) faster
    inputMaxSize = inputBuffer.length - compressionOverhead(bufferSize);
    outputBuffer = new byte[compressor.maxCompressedLength(inputMaxSize) + SIZE_OF_LONG];

    out.write(LZOP_MAGIC);

    ByteArrayOutputStream headerOut = new ByteArrayOutputStream(25);
    DataOutputStream headerDataOut = new DataOutputStream(headerOut);
    headerDataOut.writeShort(LZOP_FILE_VERSION);
    headerDataOut.writeShort(LZO_FORMAT_VERSION);
    headerDataOut.writeShort(LZOP_FORMAT_VERSION);
    headerDataOut.writeByte(LZO_1X_VARIANT);
    headerDataOut.writeByte(LEVEL);

    // flags (none)
    headerDataOut.writeInt(0);
    // file mode (non-executable regular file)
    headerDataOut.writeInt(0x81a4);
    // modified time (in seconds from epoch)
    headerDataOut.writeInt((int) (System.currentTimeMillis() / 1000));
    // time zone modifier for above, which is UTC so 0
    headerDataOut.writeInt(0);
    // file name length (none)
    headerDataOut.writeByte(0);

    byte[] header = headerOut.toByteArray();
    out.write(header);

    Adler32 headerChecksum = new Adler32();
    headerChecksum.update(header);
    writeBigEndianInt((int) headerChecksum.getValue());
  }

  @Override
  public void write(int b)
          throws IOException
  {
    inputBuffer[inputOffset++] = (byte) b;
    if (inputOffset >= inputMaxSize) {
      writeNextChunk(inputBuffer, 0, this.inputOffset);
    }
  }

  @Override
  public void write(byte[] buffer, int offset, int length)
          throws IOException
  {
    while (length > 0) {
      int chunkSize = Math.min(length, inputMaxSize - inputOffset);
      // favor writing directly from the user buffer to avoid the extra copy
      if (inputOffset == 0 && length > inputMaxSize) {
        writeNextChunk(buffer, offset, chunkSize);
      }
      else {
        System.arraycopy(buffer, offset, inputBuffer, inputOffset, chunkSize);
        inputOffset += chunkSize;

        if (inputOffset >= inputMaxSize) {
          writeNextChunk(inputBuffer, 0, inputOffset);
        }
      }
      length -= chunkSize;
      offset += chunkSize;
    }
  }

  @Override
  public void finish()
          throws IOException
  {
    if (inputOffset > 0) {
      writeNextChunk(inputBuffer, 0, this.inputOffset);
    }
  }

  @Override
  public void close()
          throws IOException
  {
    finish();
    writeBigEndianInt(0);
    super.close();
  }

  @Override
  public void resetState()
          throws IOException
  {
    finish();
  }

  private void writeNextChunk(byte[] input, int inputOffset, int inputLength)
          throws IOException
  {
    int compressedSize = compressor.compress(input, inputOffset, inputLength, outputBuffer, 0, outputBuffer.length);

    writeBigEndianInt(inputLength);
    if (compressedSize < inputLength) {
      writeBigEndianInt(compressedSize);
      out.write(outputBuffer, 0, compressedSize);
    }
    else {
      writeBigEndianInt(inputLength);
      out.write(input, inputOffset, inputLength);
    }

    this.inputOffset = 0;
  }

  private void writeBigEndianInt(int value)
          throws IOException
  {
    out.write(value >>> 24);
    out.write(value >>> 16);
    out.write(value >>> 8);
    out.write(value);
  }

  private static int compressionOverhead(int size)
  {
    return (size / 16) + 64 + 3;
  }
}
