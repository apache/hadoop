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

package org.apache.hadoop.fs.aliyun.oss;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;

/**
 * The input stream for OSS blob system.
 * The class uses multi-part downloading to read data from the object content
 * stream.
 */
public class AliyunOSSInputStream extends FSInputStream {
  public static final Log LOG = LogFactory.getLog(AliyunOSSInputStream.class);
  private static final int MAX_RETRIES = 10;
  private final long downloadPartSize;

  private String bucketName;
  private String key;
  private OSSClient ossClient;
  private Statistics statistics;
  private boolean closed;
  private InputStream wrappedStream = null;
  private long dataLen;
  private long position;
  private long partRemaining;

  public AliyunOSSInputStream(Configuration conf, OSSClient client,
      String bucketName, String key, Long dataLen, Statistics statistics)
      throws IOException {
    this.bucketName = bucketName;
    this.key = key;
    ossClient = client;
    this.statistics = statistics;
    this.dataLen = dataLen;
    downloadPartSize = conf.getLong(MULTIPART_DOWNLOAD_SIZE_KEY,
        MULTIPART_DOWNLOAD_SIZE_DEFAULT);
    reopen(0);
    closed = false;
  }

  /**
   * Reopen the wrapped stream at give position, by seeking for
   * data of a part length from object content stream.
   *
   * @param pos position from start of a file
   * @throws IOException if failed to reopen
   */
  private synchronized void reopen(long pos) throws IOException {

    long partLen;

    if (pos < 0) {
      throw new EOFException("Cannot seek at negtive position:" + pos);
    } else if (pos > dataLen) {
      throw new EOFException("Cannot seek after EOF, fileLen:" + dataLen +
          " position:" + pos);
    } else if (pos + downloadPartSize > dataLen) {
      partLen = dataLen - pos;
    } else {
      partLen = downloadPartSize;
    }

    if (wrappedStream != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Aborting old stream to open at pos " + pos);
      }
      wrappedStream.close();
    }

    GetObjectRequest request = new GetObjectRequest(bucketName, key);
    request.setRange(pos, pos + partLen - 1);
    wrappedStream = ossClient.getObject(request).getObjectContent();
    if (wrappedStream == null) {
      throw new IOException("Null IO stream");
    }
    position = pos;
    partRemaining = partLen;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();

    if (partRemaining <= 0 && position < dataLen) {
      reopen(position);
    }

    int tries = MAX_RETRIES;
    boolean retry;
    int byteRead = -1;
    do {
      retry = false;
      try {
        byteRead = wrappedStream.read();
      } catch (Exception e) {
        handleReadException(e, --tries);
        retry = true;
      }
    } while (retry);
    if (byteRead >= 0) {
      position++;
      partRemaining--;
    }

    if (statistics != null && byteRead >= 0) {
      statistics.incrementBytesRead(1);
    }
    return byteRead;
  }


  /**
   * Check whether the input stream is closed.
   *
   * @throws IOException if stream is closed
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed!");
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    checkNotClosed();

    if (buf == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > buf.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int bytesRead = 0;
    // Not EOF, and read not done
    while (position < dataLen && bytesRead < len) {
      if (partRemaining == 0) {
        reopen(position);
      }

      int tries = MAX_RETRIES;
      boolean retry;
      int bytes = -1;
      do {
        retry = false;
        try {
          bytes = wrappedStream.read(buf, off + bytesRead, len - bytesRead);
        } catch (Exception e) {
          handleReadException(e, --tries);
          retry = true;
        }
      } while (retry);

      if (bytes > 0) {
        bytesRead += bytes;
        position += bytes;
        partRemaining -= bytes;
      } else if (partRemaining != 0) {
        throw new IOException("Failed to read from stream. Remaining:" +
            partRemaining);
      }
    }

    if (statistics != null && bytesRead > 0) {
      statistics.incrementBytesRead(bytesRead);
    }

    // Read nothing, but attempt to read something
    if (bytesRead == 0 && len > 0) {
      return -1;
    } else {
      return bytesRead;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (wrappedStream != null) {
      wrappedStream.close();
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = dataLen - position;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkNotClosed();
    if (position == pos) {
      return;
    } else if (pos > position && pos < position + partRemaining) {
      wrappedStream.skip(pos - position);
      position = pos;
    } else {
      reopen(pos);
    }
  }

  @Override
  public long getPos() throws IOException {
    checkNotClosed();
    return position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    checkNotClosed();
    return false;
  }

  private void handleReadException(Exception e, int tries) throws IOException{
    if (tries == 0) {
      throw new IOException(e);
    }

    LOG.warn("Some exceptions occurred in oss connection, try to reopen oss" +
        " connection at position '" + position + "', " + e.getMessage());
    try {
      Thread.sleep(100);
    } catch (InterruptedException e2) {
      LOG.warn(e2.getMessage());
    }
    reopen(position);
  }
}
