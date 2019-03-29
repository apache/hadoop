/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.fs.FSProtos.FileStatusProto;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.protocolPB.PBHelper;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * File Status of the Ozone Key.
 */
public class OzoneFileStatus extends FileStatus {

  private static final long serialVersionUID = 1L;

  transient private OmKeyInfo keyInfo;

  public OzoneFileStatus(OmKeyInfo key, long blockSize, boolean isDirectory) {
    super(key.getDataSize(), isDirectory, key.getFactor().getNumber(),
        blockSize, key.getModificationTime(), getPath(key.getKeyName()));
    keyInfo = key;
  }

  public OzoneFileStatus(FileStatus status) throws IOException {
    super(status);
  }

  // Use this constructor only for directories
  public OzoneFileStatus(String keyName) {
    super(0, true, 0, 0, 0, getPath(keyName));
  }

  public FileStatusProto getProtobuf() throws IOException {
    return PBHelper.convert(this);
  }

  public static OzoneFileStatus getFromProtobuf(FileStatusProto response)
      throws IOException {
    return new OzoneFileStatus(PBHelper.convert(response));
  }

  public static Path getPath(String keyName) {
    return new Path(OZONE_URI_DELIMITER + keyName);
  }

  public FileStatus makeQualified(URI defaultUri, Path parent,
                                  String owner, String group) {
    // fully-qualify path
    setPath(parent.makeQualified(defaultUri, null));
    setGroup(group);
    setOwner(owner);
    if (isDirectory()) {
      setPermission(FsPermission.getDirDefault());
    } else {
      setPermission(FsPermission.getFileDefault());
    }
    return this; // API compatibility
  }

  /** Get the modification time of the file/directory.
   *
   * o3fs uses objects as "fake" directories, which are not updated to
   * reflect the accurate modification time. We choose to report the
   * current time because some parts of the ecosystem (e.g. the
   * HistoryServer) use modification time to ignore "old" directories.
   *
   * @return for files the modification time in milliseconds since January 1,
   *         1970 UTC or for directories the current time.
   */
  @Override
  public long getModificationTime(){
    if (isDirectory()) {
      return System.currentTimeMillis();
    } else {
      return super.getModificationTime();
    }
  }

  public OmKeyInfo getKeyInfo() {
    return keyInfo;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}