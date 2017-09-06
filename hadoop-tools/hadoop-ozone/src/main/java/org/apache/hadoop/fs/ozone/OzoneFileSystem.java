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

package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Objects;

import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_URI_SCHEME;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_HTTP_SCHEME;

/**
 * The Ozone Filesystem implementation.
 *
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to create
 * one. If cast to {@link OzoneFileSystem}, extra methods and features may be
 * accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFileSystem extends FileSystem {
  static final Logger LOG = LoggerFactory.getLogger(OzoneFileSystem.class);

  /** The Ozone client for connecting to Ozone server. */
  private OzoneRestClient ozone;
  private OzoneBucket bucket;
  private URI uri;
  private String userName;
  private Path workingDir;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    Objects.requireNonNull(name.getScheme(), "No scheme provided in " + name);
    assert getScheme().equals(name.getScheme());

    uri = name;
    Path path = new Path(name.getPath());
    String hostStr = name.getAuthority();
    String volumeStr = null;
    String bucketStr = null;

    LOG.info("Ozone URI for ozfs initialization is " + uri);
    while (path != null && !path.isRoot()) {
      bucketStr = volumeStr;
      volumeStr = path.getName();
      path = path.getParent();
    }

    if (hostStr == null) {
      throw new IllegalArgumentException("No host provided in " + name);
    } else if (volumeStr == null) {
      throw new IllegalArgumentException("No volume provided in " + name);
    } else if (bucketStr == null) {
      throw new IllegalArgumentException("No bucket provided in " + name);
    }

    try {
      this.ozone = new OzoneRestClient(OZONE_HTTP_SCHEME + hostStr);
      try {
        this.userName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        this.userName = OZONE_DEFAULT_USER;
      }
      this.ozone.setUserAuth(userName);

      OzoneVolume volume = ozone.getVolume(volumeStr);
      this.bucket = volume.getBucket(bucketStr);
      this.workingDir = new Path(OZONE_USER_DIR, this.userName)
              .makeQualified(this.uri, this.workingDir);
    } catch (OzoneException oe) {
      final String msg = "Ozone server exception when initializing file system";
      LOG.error(msg, oe);
      throw new IOException(msg, oe);
    } catch (URISyntaxException ue) {
      final String msg = "Invalid Ozone endpoint " + name;
      LOG.error(msg, ue);
      throw new IOException(msg, ue);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      ozone.close();
    } finally {
      super.close();
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return OZONE_URI_SCHEME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return null;
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return null;
  }

  @Override
  public String toString() {
    return "OzoneFileSystem{URI=" + uri + ", "
        + "workingDir=" + workingDir + ", "
        + "userName=" + userName + ", "
        + "statistics=" + statistics
        + "}";
  }
}
