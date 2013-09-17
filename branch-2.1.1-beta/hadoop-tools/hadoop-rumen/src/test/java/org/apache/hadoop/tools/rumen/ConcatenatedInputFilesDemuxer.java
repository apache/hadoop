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
package org.apache.hadoop.tools.rumen;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConcatenatedInputFilesDemuxer implements InputDemuxer {
  private String name;
  private DelimitedInputStream input;

  private String knownNextFileName = null;

  static private int MAXIMUM_HEADER_LINE_LENGTH = 500;

  @Override
  public void bindTo(Path path, Configuration conf) throws IOException {
    InputStream underlyingInput = null;

    if (name != null) { // re-binding before the previous one was consumed.
      close();
    }
    name = path.getName();

    underlyingInput = new PossiblyDecompressedInputStream(path, conf);

    input =
        new DelimitedInputStream(new BufferedInputStream(underlyingInput),
            "\f!!FILE=", "!!\n");

    knownNextFileName = input.nextFileName();

    if (knownNextFileName == null) {
      close();

      return;
    }

    /*
     * We handle files in specialized formats by trying their demuxers first,
     * not by failing here.
     */
    return;
  }

  @Override
  public Pair<String, InputStream> getNext() throws IOException {
    if (knownNextFileName != null) {
      Pair<String, InputStream> result =
          new Pair<String, InputStream>(knownNextFileName, input);

      knownNextFileName = null;

      return result;
    }

    String nextFileName = input.nextFileName();

    if (nextFileName == null) {
      return null;
    }

    return new Pair<String, InputStream>(nextFileName, input);
  }

  @Override
  public void close() throws IOException {
    if (input != null) {
      input.close();
    }
  }

  /**
   * A simple wrapper class to make any input stream delimited. It has an extra
   * method, getName.
   * 
   * The input stream should have lines that look like
   * <marker><filename><endmarker> . The text <marker> should not occur
   * elsewhere in the file. The text <endmarker> should not occur in a file
   * name.
   */
  static class DelimitedInputStream extends InputStream {
    private InputStream input;

    private boolean endSeen = false;

    private final String fileMarker;

    private final byte[] markerBytes;

    private final byte[] fileMarkerBuffer;

    private final String fileEndMarker;

    private final byte[] endMarkerBytes;

    private final byte[] fileEndMarkerBuffer;

    /**
     * Constructor.
     * 
     * @param input
     */
    public DelimitedInputStream(InputStream input, String fileMarker,
        String fileEndMarker) {
      this.input = new BufferedInputStream(input, 10000);
      this.input.mark(10000);
      this.fileMarker = fileMarker;
      this.markerBytes = this.fileMarker.getBytes();
      this.fileMarkerBuffer = new byte[this.markerBytes.length];
      this.fileEndMarker = fileEndMarker;
      this.endMarkerBytes = this.fileEndMarker.getBytes();
      this.fileEndMarkerBuffer = new byte[this.endMarkerBytes.length];
    }

    @Override
    public int read() throws IOException {
      if (endSeen) {
        return -1;
      }

      input.mark(10000);

      int result = input.read();

      if (result < 0) {
        endSeen = true;
        return result;
      }

      if (result == markerBytes[0]) {
        input.reset();

        // this might be a marker line
        int markerReadResult =
            input.read(fileMarkerBuffer, 0, fileMarkerBuffer.length);

        input.reset();

        if (markerReadResult < fileMarkerBuffer.length
            || !fileMarker.equals(new String(fileMarkerBuffer))) {
          return input.read();
        }

        return -1;
      }

      return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read(byte[], int, int)
     * 
     * This does SLIGHTLY THE WRONG THING.
     * 
     * If we run off the end of the segment then the input buffer will be
     * dirtied beyond the point where we claim to have read. If this turns out
     * to be a problem, save that data somewhere and restore it if needed.
     */
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      if (endSeen) {
        return -1;
      }

      input.mark(length + markerBytes.length + 10);

      int dataSeen = input.read(buffer, offset, length);

      byte[] extraReadBuffer = null;
      int extraActualRead = -1;

      // search for an instance of a file marker
      for (int i = offset; i < offset + dataSeen; ++i) {
        if (buffer[i] == markerBytes[0]) {
          boolean mismatch = false;

          for (int j = 1; j < Math.min(markerBytes.length, offset + dataSeen
              - i); ++j) {
            if (buffer[i + j] != markerBytes[j]) {
              mismatch = true;
              break;
            }
          }

          if (!mismatch) {
            // see if we have only a prefix of the markerBytes
            int uncheckedMarkerCharCount =
                markerBytes.length - (offset + dataSeen - i);

            if (uncheckedMarkerCharCount > 0) {
              if (extraReadBuffer == null) {
                extraReadBuffer = new byte[markerBytes.length - 1];

                extraActualRead = input.read(extraReadBuffer);
              }

              if (extraActualRead < uncheckedMarkerCharCount) {
                input.reset();
                return input.read(buffer, offset, length);
              }

              for (int j = 0; j < uncheckedMarkerCharCount; ++j) {
                if (extraReadBuffer[j] != markerBytes[markerBytes.length
                    - uncheckedMarkerCharCount + j]) {
                  input.reset();
                  return input.read(buffer, offset, length);
                }
              }
            }

            input.reset();

            if (i == offset) {
              return -1;
            }

            int result = input.read(buffer, offset, i - offset);
            return result;
          }
        }
      }

      return dataSeen;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
      return read(buffer, 0, buffer.length);
    }

    @Override
    public void close() throws IOException {
      if (endSeen) {
        input.close();
      }
    }

    String nextFileName() throws IOException {
      return nextFileName(MAXIMUM_HEADER_LINE_LENGTH);
    }

    private String nextFileName(int bufferSize) throws IOException {
      // the line can't contain a newline and must contain a form feed
      byte[] buffer = new byte[bufferSize];

      input.mark(bufferSize + 1);

      int actualRead = input.read(buffer);
      int mostRecentRead = actualRead;

      while (actualRead < bufferSize && mostRecentRead > 0) {
        mostRecentRead =
            input.read(buffer, actualRead, bufferSize - actualRead);

        if (mostRecentRead > 0) {
          actualRead += mostRecentRead;
        }
      }

      if (actualRead < markerBytes.length) {
        input.reset();
        return null;
      }

      for (int i = 0; i < markerBytes.length; ++i) {
        if (markerBytes[i] != buffer[i]) {
          input.reset();
          return null;
        }
      }

      for (int i = markerBytes.length; i < actualRead; ++i) {
        if (buffer[i] == endMarkerBytes[0]) {
          // gather the file name
          input.reset();
          // burn the marker
          if (input.read(buffer, 0, markerBytes.length) < markerBytes.length) {
            throw new IOException("Can't reread bytes I've read before.");
          }
          // get the file name
          if (input.read(buffer, 0, i - markerBytes.length) < i
              - markerBytes.length) {
            throw new IOException("Can't reread bytes I've read before.");
          }
          // burn the two exclamation points and the newline
          if (input.read(fileEndMarkerBuffer) < fileEndMarkerBuffer.length) {
            input.reset();
            return null;
          }
          for (int j = 0; j < endMarkerBytes.length; ++j) {
            if (endMarkerBytes[j] != fileEndMarkerBuffer[j]) {
              input.reset();
              return null;
            }
          }

          return new String(buffer, 0, i - markerBytes.length);
        }

        if (buffer[i] == '\n') {
          return null;
        }
      }

      // we ran off the end. Was the buffer too short, or is this all there was?
      input.reset();

      if (actualRead < bufferSize) {
        return null;
      }

      return nextFileName(bufferSize * 2);
    }
  }

}
