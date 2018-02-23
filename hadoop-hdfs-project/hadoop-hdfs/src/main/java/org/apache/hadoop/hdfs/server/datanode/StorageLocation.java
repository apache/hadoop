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

import java.util.regex.Pattern;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;


/**
 * Encapsulates the URI and storage medium that together describe a
 * storage directory.
 * The default storage medium is assumed to be DISK, if none is specified.
 *
 */
@InterfaceAudience.Private
public class StorageLocation
    implements Checkable<StorageLocation.CheckContext, VolumeCheckResult>,
               Comparable<StorageLocation> {
  private final StorageType storageType;
  private final URI baseURI;
  /** Regular expression that describes a storage uri with a storage type.
   *  e.g. [Disk]/storages/storage1/
   */
  private static final Pattern regex = Pattern.compile("^\\[(\\w*)\\](.+)$");

  private StorageLocation(StorageType storageType, URI uri) {
    this.storageType = storageType;
    if (uri.getScheme() == null || uri.getScheme().equals("file")) {
      // make sure all URIs that point to a file have the same scheme
      uri = normalizeFileURI(uri);
    }
    baseURI = uri;
  }

  public static URI normalizeFileURI(URI uri) {
    try {
      File uriFile = new File(uri.getPath());
      String uriStr = uriFile.toURI().normalize().toString();
      if (uriStr.endsWith("/")) {
        uriStr = uriStr.substring(0, uriStr.length() - 1);
      }
      return new URI(uriStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
              "URI: " + uri + " is not in the expected format");
    }
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  public URI getUri() {
    return baseURI;
  }

  public URI getNormalizedUri() {
    return baseURI.normalize();
  }

  public boolean matchesStorageDirectory(StorageDirectory sd)
      throws IOException {
    return this.equals(sd.getStorageLocation());
  }

  public boolean matchesStorageDirectory(StorageDirectory sd,
      String bpid) throws IOException {
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED &&
        storageType == StorageType.PROVIDED) {
      return matchesStorageDirectory(sd);
    }
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED ||
        storageType == StorageType.PROVIDED) {
      // only one PROVIDED storage directory can exist; so this cannot match!
      return false;
    }
    // both storage directories are local
    return this.getBpURI(bpid, Storage.STORAGE_DIR_CURRENT).normalize()
        .equals(sd.getRoot().toURI().normalize());
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
  public static StorageLocation parse(String rawLocation)
      throws IOException, SecurityException {
    Matcher matcher = regex.matcher(rawLocation);
    StorageType storageType = StorageType.DEFAULT;
    String location = rawLocation;

    if (matcher.matches()) {
      String classString = matcher.group(1);
      location = matcher.group(2).trim();
      if (!classString.isEmpty()) {
        storageType =
            StorageType.valueOf(StringUtils.toUpperCase(classString));
      }
    }
    //do Path.toURI instead of new URI(location) as this ensures that
    //"/a/b" and "/a/b/" are represented in a consistent manner
    return new StorageLocation(storageType, new Path(location).toUri());
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + baseURI.normalize();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof StorageLocation)) {
      return false;
    }
    int comp = compareTo((StorageLocation) obj);
    return comp == 0;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public int compareTo(StorageLocation obj) {
    if (obj == this) {
      return 0;
    } else if (obj == null) {
      return -1;
    }

    StorageLocation otherStorage = (StorageLocation) obj;
    if (this.getNormalizedUri() != null &&
        otherStorage.getNormalizedUri() != null) {
      return this.getNormalizedUri().compareTo(
          otherStorage.getNormalizedUri());
    } else if (this.getNormalizedUri() == null &&
        otherStorage.getNormalizedUri() == null) {
      return this.storageType.compareTo(otherStorage.getStorageType());
    } else if (this.getNormalizedUri() == null) {
      return -1;
    } else {
      return 1;
    }

  }

  public URI getBpURI(String bpid, String currentStorageDir) {
    try {
      File localFile = new File(getUri());
      return new File(new File(localFile, currentStorageDir), bpid).toURI();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Create physical directory for block pools on the data node.
   *
   * @param blockPoolID
   *          the block pool id
   * @param conf
   *          Configuration instance to use.
   * @throws IOException on errors
   */
  public void makeBlockPoolDir(String blockPoolID,
      Configuration conf) throws IOException {

    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    if (storageType == StorageType.PROVIDED) {
      // skip creation if the storage type is PROVIDED
      Storage.LOG.info("Skipping creating directory for block pool "
          + blockPoolID + " for PROVIDED storage location " + this);
      return;
    }

    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(conf.get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    File data = new File(getBpURI(blockPoolID, Storage.STORAGE_DIR_CURRENT));
    try {
      DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
    } catch (IOException e) {
      DataStorage.LOG.warn("Invalid directory in: " + data.getCanonicalPath() +
          ": " + e.getMessage());
    }
  }

  @Override  // Checkable
  public VolumeCheckResult check(CheckContext context) throws IOException {
    // assume provided storage locations are always healthy,
    // and check only for local storages.
    if (storageType != StorageType.PROVIDED) {
      DiskChecker.checkDir(
          context.localFileSystem,
          new Path(baseURI),
          context.expectedPermission);
    }
    return VolumeCheckResult.HEALTHY;
  }

  /**
   * Class to hold the parameters for running a {@link #check}.
   */
  public static final class CheckContext {
    private final LocalFileSystem localFileSystem;
    private final FsPermission expectedPermission;

    public CheckContext(LocalFileSystem localFileSystem,
                        FsPermission expectedPermission) {
      this.localFileSystem = localFileSystem;
      this.expectedPermission = expectedPermission;
    }
  }
}
