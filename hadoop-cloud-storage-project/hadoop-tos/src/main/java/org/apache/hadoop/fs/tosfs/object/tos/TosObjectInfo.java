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

package org.apache.hadoop.fs.tosfs.object.tos;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;

import java.util.Date;
import java.util.Objects;

public class TosObjectInfo extends ObjectInfo {
  private final String crc64ecma;
  private final boolean appendable;

  public TosObjectInfo(String key, long size, Date mtime, byte[] checksum, boolean isDir,
      boolean appendable, String crc64ecma) {
    super(key, size, mtime, checksum, isDir);
    this.crc64ecma = crc64ecma;
    this.appendable = appendable;
  }

  public String crc64ecma() {
    return crc64ecma;
  }

  public boolean appendable() {
    return appendable;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    if (!(o instanceof TosObjectInfo)) {
      return false;
    }

    TosObjectInfo that = (TosObjectInfo) o;
    return Objects.equals(appendable, that.appendable) && Objects.equals(crc64ecma, that.crc64ecma);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), appendable, crc64ecma);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key())
        .add("size", size())
        .add("mtime", mtime())
        .add("checksum", Hex.encodeHexString(checksum()))
        .add("isDir", isDir())
        .add("appendable", appendable)
        .add("crc64ecma", crc64ecma)
        .toString();
  }
}
