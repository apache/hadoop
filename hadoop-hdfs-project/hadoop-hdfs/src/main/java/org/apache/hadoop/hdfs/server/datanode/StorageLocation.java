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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.common.Util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;

/**
 * Encapsulates the URI and storage medium that together describe a
 * storage directory.
 * The default storage medium is assumed to be DISK, if none is specified.
 *
 */
@InterfaceAudience.Private
public class StorageLocation {
  public static final Log LOG = LogFactory.getLog(StorageLocation.class);
  final StorageType storageType;
  final File file;

  // Regular expression that describes a storage uri with a storage type.
  // e.g. [Disk]/storages/storage1/
  private static final String rawStringRegex = "^\\[(\\w*)\\](.+)$";

  StorageLocation(URI uri) {
    this(StorageType.DISK, uri);
  }

  StorageLocation(StorageType storageType, URI uri) {
    this.storageType = storageType;

    if (uri.getScheme() == null ||
        "file".equalsIgnoreCase(uri.getScheme())) {
      // drop any (illegal) authority in the URI for backwards compatibility
      this.file = new File(uri.getPath());
    } else {
      throw new IllegalArgumentException(
          "Got an Unsupported URI schema in " + uri + ". Ignoring ...");
    }
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  public URI getUri() {
    return file.toURI();
  }

  public File getFile() {
    return this.file;
  }

  /**
   * Attempt to parse a storage uri with storage class and URI. The storage
   * class component of the uri is case-insensitive.
   *
   * @param rawLocation Location string of the format [type]uri, where [type] is
   *                    optional.
   * @return A StorageLocation object if successfully parsed, null otherwise.
   *         Does not throw any exceptions.
   */
  public static StorageLocation parse(String rawLocation) throws IOException {
    Matcher matcher = compile(rawStringRegex).matcher(rawLocation);
    StorageType storageType = StorageType.DISK;
    String location = rawLocation;

    if (matcher.matches()) {
      String classString = matcher.group(1);
      location = matcher.group(2);
      if (!classString.isEmpty()) {
        try {
          storageType = StorageType.valueOf(classString.toUpperCase());
        } catch (RuntimeException re) {
          LOG.error("Unable to parse storage type: " + re.toString() +
                    ". Using the default storage type for directory " +
                    location);
        }
      }
    }

    return new StorageLocation(storageType, Util.stringAsURI(location));
  }

  public String toString() {
    return "[" + storageType.toString() + "]" + file.toURI().toString();
  }
}
