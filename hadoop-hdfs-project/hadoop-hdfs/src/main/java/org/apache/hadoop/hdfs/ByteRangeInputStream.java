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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;

/**
 * To support HTTP byte streams, a new connection to an HTTP server needs to be
 * created each time. This class hides the complexity of those multiple 
 * connections from the client. Whenever seek() is called, a new connection
 * is made on the successive read(). The normal input stream functions are 
 * connected to the currently active input stream. 
 */
public abstract class ByteRangeInputStream extends FSInputStream {
  
  /**
   * This class wraps a URL and provides method to open connection.
   * It can be overridden to change how a connection is opened.
   */
  public static abstract class URLOpener {
    protected URL url;
  
    public URLOpener(URL u) {
      url = u;
    }
  
    public void setURL(URL u) {
      url = u;
    }
  
    public URL getURL() {
      return url;
    }

    protected abstract HttpURLConnection openConnection() throws IOException;

    protected abstract HttpURLConnection openConnection(final long offset) throws IOException;
  }

  enum StreamStatus {
    NORMAL, SEEK
  }
  protected InputStream in;
  protected URLOpener originalURL;
  protected URLOpener resolvedURL;
  protected long startPos = 0;
  protected long currentPos = 0;
  protected long filelength;

  StreamStatus status = StreamStatus.SEEK;

  /**
   * Create with the specified URLOpeners. Original url is used to open the 
   * stream for the first time. Resolved url is used in subsequent requests.
   * @param o Original url
   * @param r Resolved url
   */
  public ByteRangeInputStream(URLOpener o, URLOpener r) {
    this.originalURL = o;
    this.resolvedURL = r;
  }
  
  protected abstract void checkResponseCode(final HttpURLConnection connection
      ) throws IOException;
  
  protected abstract URL getResolvedUrl(final HttpURLConnection connection
      ) throws IOException;

  private InputStream getInputStream() throws IOException {
    if (status != StreamStatus.NORMAL) {
      
      if (in != null) {
        in.close();
        in = null;
      }
      
      // Use the original url if no resolved url exists, eg. if
      // it's the first time a request is made.
      final URLOpener opener =
        (resolvedURL.getURL() == null) ? originalURL : resolvedURL;

      final HttpURLConnection connection = opener.openConnection(startPos);
      connection.connect();
      checkResponseCode(connection);

      final String cl = connection.getHeaderField(StreamFile.CONTENT_LENGTH);
      filelength = (cl == null) ? -1 : Long.parseLong(cl);
      in = connection.getInputStream();

      resolvedURL.setURL(getResolvedUrl(connection));
      status = StreamStatus.NORMAL;
    }
    
    return in;
  }
  
  private void update(final boolean isEOF, final int n)
      throws IOException {
    if (!isEOF) {
      currentPos += n;
    } else if (currentPos < filelength) {
      throw new IOException("Got EOF but currentPos = " + currentPos
          + " < filelength = " + filelength);
    }
  }

  public int read() throws IOException {
    final int b = getInputStream().read();
    update(b == -1, 1);
    return b;
  }
  
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  public void seek(long pos) throws IOException {
    if (pos != currentPos) {
      startPos = pos;
      currentPos = pos;
      status = StreamStatus.SEEK;
    }
  }

  /**
   * Return the current offset from the start of the file
   */
  public long getPos() throws IOException {
    return currentPos;
  }

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   */
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}