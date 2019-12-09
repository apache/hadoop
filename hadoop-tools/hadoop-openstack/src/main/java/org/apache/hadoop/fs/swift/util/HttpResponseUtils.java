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

package org.apache.hadoop.fs.swift.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.util.EncodingUtils;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.HEADER_CONTENT_LENGTH;

/**
 * Utility class for parsing HttpResponse. This class is implemented like
 * {@code org.apache.commons.httpclient.HttpMethodBase.java} in httpclient 3.x.
 */
public abstract class HttpResponseUtils {

  /**
   * Returns the response body of the HTTPResponse, if any, as an array of bytes.
   * If response body is not available or cannot be read, returns <tt>null</tt>
   *
   * Note: This will cause the entire response body to be buffered in memory. A
   * malicious server may easily exhaust all the VM memory. It is strongly
   * recommended, to use getResponseAsStream if the content length of the
   * response is unknown or reasonably large.
   *
   * @param resp HttpResponse
   * @return The response body
   * @throws IOException If an I/O (transport) problem occurs while obtaining
   * the response body.
   */
  public static byte[] getResponseBody(HttpResponse resp) throws IOException {
    try(InputStream instream = resp.getEntity().getContent()) {
      if (instream != null) {
        long contentLength = resp.getEntity().getContentLength();
        if (contentLength > Integer.MAX_VALUE) {
          //guard integer cast from overflow
          throw new IOException("Content too large to be buffered: "
              + contentLength +" bytes");
        }
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(
            contentLength > 0 ? (int) contentLength : 4*1024);
        byte[] buffer = new byte[4096];
        int len;
        while ((len = instream.read(buffer)) > 0) {
          outstream.write(buffer, 0, len);
        }
        outstream.close();
        return outstream.toByteArray();
      }
    }
    return null;
  }

  /**
   * Returns the response body of the HTTPResponse, if any, as a {@link String}.
   * If response body is not available or cannot be read, returns <tt>null</tt>
   * The string conversion on the data is done using UTF-8.
   *
   * Note: This will cause the entire response body to be buffered in memory. A
   * malicious server may easily exhaust all the VM memory. It is strongly
   * recommended, to use getResponseAsStream if the content length of the
   * response is unknown or reasonably large.
   *
   * @param resp HttpResponse
   * @return The response body.
   * @throws IOException If an I/O (transport) problem occurs while obtaining
   * the response body.
   */
  public static String getResponseBodyAsString(HttpResponse resp)
      throws IOException {
    byte[] rawdata = getResponseBody(resp);
    if (rawdata != null) {
      return EncodingUtils.getString(rawdata, "UTF-8");
    } else {
      return null;
    }
  }

  /**
   * Return the length (in bytes) of the response body, as specified in a
   * <tt>Content-Length</tt> header.
   *
   * <p>
   * Return <tt>-1</tt> when the content-length is unknown.
   * </p>
   *
   * @param resp HttpResponse
   * @return content length, if <tt>Content-Length</tt> header is available.
   *          <tt>0</tt> indicates that the request has no body.
   *          If <tt>Content-Length</tt> header is not present, the method
   *          returns <tt>-1</tt>.
   */
  public static long getContentLength(HttpResponse resp) {
    Header header = resp.getFirstHeader(HEADER_CONTENT_LENGTH);
    if (header == null) {
      return -1;
    } else {
      return Long.parseLong(header.getValue());
    }
  }
}
