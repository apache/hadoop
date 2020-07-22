package org.apache.hadoop.io.compress.lzo;

import io.airlift.compress.lzo.LzoCompressor;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class LzoOutputStream
        extends CompressionOutputStream
{
  public static final int SIZE_OF_SHORT = 2;
  public static final int SIZE_OF_INT = 4;
  public static final int SIZE_OF_LONG = 8;

  private final io.airlift.compress.lzo.LzoCompressor compressor = new LzoCompressor();

  private final byte[] inputBuffer;
  private final int inputMaxSize;
  private int inputOffset;

  private final byte[] outputBuffer;

  public LzoOutputStream(OutputStream out, int bufferSize)
  {
    super(out);
    inputBuffer = new byte[bufferSize];
    // leave extra space free at end of buffers to make compression (slightly) faster
    inputMaxSize = inputBuffer.length - compressionOverhead(bufferSize);
    outputBuffer = new byte[compressor.maxCompressedLength(inputMaxSize) + SIZE_OF_LONG];
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
    writeBigEndianInt(compressedSize);
    out.write(outputBuffer, 0, compressedSize);

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
