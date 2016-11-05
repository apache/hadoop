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
package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * Utility methods used by HttpFS classes.
 */
@InterfaceAudience.Private
public class HttpFSUtils {

  public static final String SERVICE_NAME = "/webhdfs";

  public static final String SERVICE_VERSION = "/v1";

  public static final byte[] EMPTY_BYTES = {};

  private static final String SERVICE_PATH = SERVICE_NAME + SERVICE_VERSION;

  /**
   * Convenience method that creates an HTTP <code>URL</code> for the
   * HttpFSServer file system operations.
   * <p/>
   *
   * @param path the file path.
   * @param params the query string parameters.
   *
   * @return a <code>URL</code> for the HttpFSServer server,
   *
   * @throws IOException thrown if an IO error occurs.
   */
  static URL createURL(Path path, Map<String, String> params)
    throws IOException {
    return createURL(path, params, null);
  }

  /**
   * Convenience method that creates an HTTP <code>URL</code> for the
   * HttpFSServer file system operations.
   * <p/>
   *
   * @param path the file path.
   * @param params the query string parameters.
   * @param multiValuedParams multi valued parameters of the query string
   *
   * @return URL a <code>URL</code> for the HttpFSServer server,
   *
   * @throws IOException thrown if an IO error occurs.
   */
  static URL createURL(Path path, Map<String, String> params, Map<String, 
      List<String>> multiValuedParams) throws IOException {
    URI uri = path.toUri();
    String realScheme;
    if (uri.getScheme().equalsIgnoreCase(HttpFSFileSystem.SCHEME)) {
      realScheme = "http";
    } else if (uri.getScheme().equalsIgnoreCase(HttpsFSFileSystem.SCHEME)) {
      realScheme = "https";

    } else {
      throw new IllegalArgumentException(MessageFormat.format(
        "Invalid scheme [{0}] it should be '" + HttpFSFileSystem.SCHEME + "' " +
            "or '" + HttpsFSFileSystem.SCHEME + "'", uri));
    }
    StringBuilder sb = new StringBuilder();
    sb.append(realScheme).append("://").append(uri.getAuthority()).
      append(SERVICE_PATH).append(uri.getPath());

    String separator = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(separator).append(entry.getKey()).append("=").
        append(URLEncoder.encode(entry.getValue(), "UTF8"));
      separator = "&";
    }
    if (multiValuedParams != null) {
      for (Map.Entry<String, List<String>> multiValuedEntry : 
        multiValuedParams.entrySet()) {
        String name = URLEncoder.encode(multiValuedEntry.getKey(), "UTF8");
        List<String> values = multiValuedEntry.getValue();
        for (String value : values) {
          sb.append(separator).append(name).append("=").
            append(URLEncoder.encode(value, "UTF8"));
          separator = "&";
        }
      }
    }
    return new URL(sb.toString());
  }

  /**
   * Convenience method that JSON Parses the <code>InputStream</code> of a
   * <code>HttpURLConnection</code>.
   *
   * @param conn the <code>HttpURLConnection</code>.
   *
   * @return the parsed JSON object.
   *
   * @throws IOException thrown if the <code>InputStream</code> could not be
   * JSON parsed.
   */
  static Object jsonParse(HttpURLConnection conn) throws IOException {
    try {
      JSONParser parser = new JSONParser();
      return parser.parse(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    } catch (ParseException ex) {
      throw new IOException("JSON parser error, " + ex.getMessage(), ex);
    }
  }
}
