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

/**
 * There are two kinds of bucket types: general purpose bucket(common bucket) and directory bucket.
 * Directory bucket organize data hierarchically into directories as opposed to the flat storage
 * structure of general purpose buckets.
 * <p>
 * Only a few object storages support directory bucket, e.g. S3, OBS, TOS. Generally, directory
 * bucket supports rename or delete dir with constant time complexity O(1), but these object
 * storages have slight differences on these APIs. E.g. S3 doesn't provide rename API. S3 will
 * recursively delete any empty parent directories in the object path during delete an object in a
 * directory bucket, but TOS won't delete any empty parent directories.
 * <p>
 * And also there are some general difference between general purpose bucket and directory bucket.
 * <ul>
 *   <li>Directory bucket treats the object end with '/' as the directory during creating object.
 *   </li>
 *   <li>TOS directory bucket will create missed parent dir during create object automatically,
 *   but general purpose bucket only create one object.</li>
 *   <li>Directory bucket doesn't allow create any object under a file, but general purpose bucket
 *   haven't this constraint.</li>
 *   <li>If a object 'a/b/' exists in directory bucket, both head('a/b') and head('a/b/') will get
 *   the object meta, but only head('a/b/') can get the object meta from general purpose bucket</li>
 *   <li>TOS directory bucket provides atomic rename/delete dir abilities</li>
 * </ul>
 */
public class BucketInfo {
  private final String name;
  private final boolean directory;

  public BucketInfo(String name, boolean directory) {
    this.name = name;
    this.directory = directory;
  }

  public String name() {
    return name;
  }

  public boolean isDirectory() {
    return directory;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof BucketInfo)) {
      return false;
    }

    BucketInfo that = (BucketInfo) o;
    return Objects.equals(name, that.name)
        && Objects.equals(directory, that.directory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, directory);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("directory", directory)
        .toString();
  }
}
