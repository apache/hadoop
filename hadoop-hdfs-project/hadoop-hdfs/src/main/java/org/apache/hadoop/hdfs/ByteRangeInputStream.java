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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;

/**
 * To support HTTP byte streams, a new connection to an HTTP server needs to be
 * created each time. This class hides the complexity of those multiple 
 * connections from the client. Whenever seek() is called, a new connection
 * is made on the successive read(). The normal input stream functions are 
 * connected to the currently active input stream. 
 */
public class ByteRangeInputStream extends FSInputStream {
  
  /**
   * This class wraps a URL and provides method to open connection.
   * It can be overridden to change how a connection is opened.
   */
  public static class URLOpener {
    protected URL url;
    /** The url with offset parameter */
    protected URL offsetUrl;
  
    public URLOpener(URL u) {
      url = u;
    }
  
    public void setURL(URL u) {
      url = u;
    }
  
    public URL getURL() {
      return url;
    }

    protected HttpURLConnection openConnection() throws IOException {
      return (HttpURLConnection)offsetUrl.openConnection();
    }

    private HttpURLConnection openConnection(final long offset) throws IOException {
      offsetUrl = offset == 0L? url: new URL(url + "&" + new OffsetParam(offset));
      final HttpURLConnection conn = openConnection();
      conn.setRequestMethod("GET");
      if (offset != 0L) {
        conn.setRequestProperty("Range", "bytes=" + offset + "-");
      }
      return conn;
    }  
  }
  
  static private final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";

  /** Remove offset parameter, if there is any, from the url */
  static URL removeOffsetParam(final URL url) throws MalformedURLException {
    String query = url.getQuery();
    if (query == null) {
      return url;
    }
    final String lower = query.toLowerCase();
    if (!lower.startsWith(OFFSET_PARAM_PREFIX)
        && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
      return url;
    }

    //rebuild query
    StringBuilder b = null;
    for(final StringTokenizer st = new StringTokenizer(query, "&");
        st.hasMoreTokens();) {
      final String token = st.nextToken();
      if (!token.toLowerCase().startsWith(OFFSET_PARAM_PREFIX)) {
        if (b == null) {
          b = new StringBuilder("?").append(token);
        } else {
          b.append('&').append(token);
        }
      }
    }
    query = b == null? "": b.toString();

    final String urlStr = url.toString();
    return new URL(urlStr.substring(0, urlStr.indexOf('?')) + query);
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

  /** Create an input stream with the URL. */
  public ByteRangeInputStream(final URL url) {
    this(new URLOpener(url), new URLOpener(null));
  }
  
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
      try {
        connection.connect();
        final String cl = connection.getHeaderField(StreamFile.CONTENT_LENGTH);
        filelength = (cl == null) ? -1 : Long.parseLong(cl);
        if (HftpFileSystem.LOG.isDebugEnabled()) {
          HftpFileSystem.LOG.debug("filelength = " + filelength);
        }
        in = connection.getInputStream();
      } catch (FileNotFoundException fnfe) {
        throw fnfe;
      } catch (IOException ioe) {
        HftpFileSystem.throwIOExceptionFromConnection(connection, ioe);
      }
      
      int respCode = connection.getResponseCode();
      if (startPos != 0 && respCode != HttpURLConnection.HTTP_PARTIAL) {
        // We asked for a byte range but did not receive a partial content
        // response...
        throw new IOException("HTTP_PARTIAL expected, received " + respCode);
      } else if (startPos == 0 && respCode != HttpURLConnection.HTTP_OK) {
        // We asked for all bytes from the beginning but didn't receive a 200
        // response (none of the other 2xx codes are valid here)
        throw new IOException("HTTP_OK expected, received " + respCode);
      }

      resolvedURL.setURL(removeOffsetParam(connection.getURL()));
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