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

import java.util.Objects;

public class Part {
  private int num;
  private long size;
  private String eTag;

  // No-arg constructor for json serializer, don't use.
  public Part() {
  }

  public Part(int num, long size, String eTag) {
    this.num = num;
    this.size = size;
    this.eTag = eTag;
  }

  public int num() {
    return num;
  }

  public long size() {
    return size;
  }

  public String eTag() {
    return eTag;
  }

  @Override
  public int hashCode() {
    return Objects.hash(num, size, eTag);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Part)) {
      return false;
    }
    Part that = (Part) o;
    return Objects.equals(num, that.num)
        && Objects.equals(size, that.size)
        && Objects.equals(eTag, that.eTag);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("PartNum", num)
        .add("PartSize", size)
        .add("ETag", eTag)
        .toString();
  }
}
