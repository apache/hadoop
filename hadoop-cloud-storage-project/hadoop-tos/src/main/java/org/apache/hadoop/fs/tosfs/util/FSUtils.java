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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Preconditions;

import java.net.URI;

public final class FSUtils {
  private static final String OVERFLOW_ERROR_HINT = FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
      + ": request length = %s, with offset = %s, buffer capacity = %s";

  private FSUtils() {
  }

  public static void checkReadParameters(byte[] buffer, int offset, int length) {
    Preconditions.checkArgument(buffer != null, "Null buffer");
    Preconditions.checkArgument(offset >= 0 && offset <= buffer.length,
        "offset: %s is out of range [%s, %s]", offset, 0, buffer.length);
    Preconditions.checkArgument(length >= 0, "length: %s is negative", length);
    Preconditions.checkArgument(buffer.length >= offset + length,
        OVERFLOW_ERROR_HINT, length, offset, (buffer.length - offset));
  }

  public static URI normalizeURI(URI fsUri, Configuration hadoopConfig) {
    final String scheme = fsUri.getScheme();
    final String authority = fsUri.getAuthority();

    if (scheme == null && authority == null) {
      fsUri = FileSystem.getDefaultUri(hadoopConfig);
    } else if (scheme != null && authority == null) {
      URI defaultUri = FileSystem.getDefaultUri(hadoopConfig);
      if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
        fsUri = defaultUri;
      }
    }
    return fsUri;
  }

  public static String scheme(Configuration conf, URI uri) {
    if (uri.getScheme() == null || uri.getScheme().isEmpty()) {
      return FileSystem.getDefaultUri(conf).getScheme();
    } else {
      return uri.getScheme();
    }
  }
}
