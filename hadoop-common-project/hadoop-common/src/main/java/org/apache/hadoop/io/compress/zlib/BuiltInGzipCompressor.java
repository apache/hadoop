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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.DataChecksum;

/**
 * A {@link Compressor} based on the popular gzip compressed file format.
 * http://www.gzip.org/
 *
 */
@DoNotPool
public class BuiltInGzipCompressor implements Compressor {

    /**
     * Fixed ten-byte gzip header. See {@link GZIPOutputStream}'s source for
     * details.
     */
    private static final byte[] GZIP_HEADER = new byte[] {
            0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

    // 'true' (nowrap) => Deflater will handle raw deflate stream only
    private Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);

    private int headerOff = 0;

    private byte[] userBuf = null;
    private int userBufOff = 0;
    private int userBufLen = 0;

    private int headerBytesWritten = 0;
    private int trailerBytesWritten = 0;

    private long currentInputLen = 0;

    private Checksum crc = DataChecksum.newCrc32();

    private BuiltInGzipDecompressor.GzipStateLabel state;

    public BuiltInGzipCompressor(Configuration conf) {
        ZlibCompressor.CompressionLevel level = ZlibFactory.getCompressionLevel(conf);
        ZlibCompressor.CompressionStrategy strategy = ZlibFactory.getCompressionStrategy(conf);

        deflater = new Deflater(level.compressionLevel(), true);
        deflater.setStrategy(strategy.compressionStrategy());

        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
    }

    @Override
    public boolean finished() {
        return deflater.finished();
    }

    @Override
    public boolean needsInput() {
        if (state == BuiltInGzipDecompressor.GzipStateLabel.DEFLATE_STREAM) {
            return deflater.needsInput();
        }

        return (state != BuiltInGzipDecompressor.GzipStateLabel.FINISHED);
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
        int numAvailBytes = 0;

        // If we are not within uncompression data yet. Output the header.
        if (state != BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
            int outputHeaderSize = writeHeader(b, off, len);

            // Completes header output.
            if (headerOff == 10) {
                state = BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM;
            }

            numAvailBytes += outputHeaderSize;

            if (outputHeaderSize == len) {
                return numAvailBytes;
            }

            off += outputHeaderSize;
            len -= outputHeaderSize;
        }

        if (state == BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
            if (!deflater.finished()) {
                // hand off user data (or what's left of it) to Deflater--but note that
                // Deflater may not have consumed all of previous bufferload, in which case
                // userBufLen will be zero
                if (userBufLen > 0) {
                    deflater.setInput(userBuf, userBufOff, userBufLen);

                    crc.update(userBuf, userBufOff, userBufLen);  // CRC-32 is on uncompressed data

                    currentInputLen = userBufLen;
                    userBufOff += userBufLen;
                    userBufLen = 0;
                }

                // now compress it into b[]
                long beforeDeflate = deflater.getBytesRead();
                numAvailBytes += deflater.deflate(b, off, len - 8);

                off += numAvailBytes;
                len -= numAvailBytes;

                // All current input are processed. Going to output trailer.
                if (deflater.getBytesRead() == currentInputLen) {
                    state = BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC;
                } else {
                    return numAvailBytes;
                }
            } else {
                throw new IOException("Cannot write more data, " +
                        "the end of the compressed data stream has been reached");
            }
        }


        numAvailBytes += writeTrailer(b, off, len);

        return numAvailBytes;
    }

    @Override
    public long getBytesRead() {
        return deflater.getTotalIn();
    }

    @Override
    public long getBytesWritten() {
        return headerBytesWritten + deflater.getTotalOut() + trailerBytesWritten;
    }

    @Override
    public void end() { deflater.end(); }

    @Override
    public void finish() { deflater.finish(); }

    @Override
    public void reinit(Configuration conf) {
        deflater.reset();
        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
        userBufOff = userBufLen = 0;
        headerBytesWritten = trailerBytesWritten = 0;
        currentInputLen = 0;
    }

    @Override
    public void reset() {
        deflater.reset();
        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
        userBufOff = userBufLen = 0;
        currentInputLen = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
        deflater.setDictionary(b, off, len);
    }

    @Override
    public void setInput(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        userBuf = b;
        userBufOff = off;
        userBufLen = len;
    }

    private int writeHeader(byte[] b, int off, int len) {
        if (len <= 0) {
            return 0;
        }

        int n = Math.min(len, 10 - headerOff);
        System.arraycopy(GZIP_HEADER, headerOff, b, off, n);
        headerOff += n;

        return n;
    }

    private int writeTrailer(byte[] b, int off, int len) {
        int writtenSize = 0;

        if (len >= 4 && state == BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC) {
            int streamCrc = (int) crc.getValue();
            b[off] = (byte) streamCrc;
            b[off + 1] = (byte) (streamCrc >> 8);
            b[off + 2] = (byte) (streamCrc >> 16);
            b[off + 3] = (byte) (streamCrc >> 24);

            len -= 4;
            off += 4;

            writtenSize += 4;
            state = BuiltInGzipDecompressor.GzipStateLabel.TRAILER_SIZE;
        }

        if (len >= 4 && state == BuiltInGzipDecompressor.GzipStateLabel.TRAILER_SIZE) {
            int totalIn = deflater.getTotalIn();

            b[off] = (byte) totalIn;
            b[off + 1] = (byte) (totalIn >> 8);
            b[off + 2] = (byte) (totalIn >> 16);
            b[off + 3] = (byte) (totalIn >> 24);

            len -= 4;
            off += 4;
            writtenSize += 4;

            state = BuiltInGzipDecompressor.GzipStateLabel.FINISHED;
        }

        return writtenSize;
    }
}
