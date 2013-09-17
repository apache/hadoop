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

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Utilities for handling URLs
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class URLUtils {
  /**
   * Timeout for socket connects and reads
   */
  public static int SOCKET_TIMEOUT = 1*60*1000; // 1 minute

  /**
   * Opens a url with read and connect timeouts
   * @param url to open
   * @return URLConnection
   * @throws IOException
   */
  public static URLConnection openConnection(URL url) throws IOException {
    URLConnection connection = url.openConnection();
    setTimeouts(connection);
    return connection;    
  }

  /**
   * Sets timeout parameters on the given URLConnection.
   * 
   * @param connection URLConnection to set
   */
  static void setTimeouts(URLConnection connection) {
    connection.setConnectTimeout(SOCKET_TIMEOUT);
    connection.setReadTimeout(SOCKET_TIMEOUT);
  }
}
