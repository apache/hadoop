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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

import static org.apache.hadoop.util.Preconditions.checkArgument;

public class ObjectInfo {
  private final String key;
  private final long size;
  private final Date mtime;
  private final boolean isDir;
  private final byte[] checksum;

  public ObjectInfo(String key, long size, Date mtime, byte[] checksum) {
    this(key, size, mtime, checksum, ObjectInfo.isDir(key));
  }

  public ObjectInfo(String key, long size, Date mtime, byte[] checksum, boolean isDir) {
    checkArgument(key != null, "Key is null");
    checkArgument(size >= 0, "The size of key(%s) is negative", key);
    checkArgument(mtime != null, "The modified time of key(%s) null.", key);
    this.key = key;
    this.size = size;
    this.mtime = mtime;
    this.isDir = isDir;
    // checksum can be null since some object storage might not support checksum.
    this.checksum = checksum == null || isDir ? Constants.MAGIC_CHECKSUM : checksum;
  }

  public String key() {
    return key;
  }

  /**
   * The size of directory object is 0.
   *
   * @return the size of object.
   */
  public long size() {
    return isDir ? 0 : size;
  }

  public Date mtime() {
    return mtime;
  }

  /**
   * @return {@link Constants#MAGIC_CHECKSUM} if the object is a dir or the object storage
   * doesn't support the given checksum type.
   */
  public byte[] checksum() {
    return checksum;
  }

  public boolean isDir() {
    return isDir;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof ObjectInfo)) {
      return false;
    }

    ObjectInfo that = (ObjectInfo) o;
    return Objects.equals(key, that.key)
        && Objects.equals(size, that.size)
        && Objects.equals(mtime, that.mtime)
        && Arrays.equals(checksum, that.checksum)
        && Objects.equals(isDir, that.isDir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, size, mtime, Arrays.hashCode(checksum), isDir);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("size", size)
        .add("mtime", mtime)
        .add("checksum", StringUtils.byteToHexString(checksum))
        .add("isDir", isDir)
        .toString();
  }

  public static boolean isDir(String key) {
    return key.endsWith(Constants.SLASH);
  }
}
