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

package org.apache.hadoop.yarn.csi.utils;

import io.netty.channel.unix.DomainSocketAddress;

import java.io.File;
import java.net.SocketAddress;

/**
 * Helper classes for gRPC utility functions.
 */
public final class GrpcHelper {

  protected static final String UNIX_DOMAIN_SOCKET_PREFIX = "unix://";

  private GrpcHelper() {
    // hide constructor for utility class
  }

  public static SocketAddress getSocketAddress(String value) {
    if (value.startsWith(UNIX_DOMAIN_SOCKET_PREFIX)) {
      String filePath = value.substring(UNIX_DOMAIN_SOCKET_PREFIX.length());
      File file = new File(filePath);
      if (!file.isAbsolute()) {
        throw new IllegalArgumentException(
            "Unix domain socket file path must be absolute, file: " + value);
      }
      // Create the SocketAddress referencing the file.
      return new DomainSocketAddress(file);
    } else {
      throw new IllegalArgumentException("Given address " + value
          + " is not a valid unix domain socket path");
    }
  }
}
