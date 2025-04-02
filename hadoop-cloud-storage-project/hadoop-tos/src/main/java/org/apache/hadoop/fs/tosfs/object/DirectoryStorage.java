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

public interface DirectoryStorage extends ObjectStorage {

  /**
   * List the objects under the given dir key. Does not guarantee to return the list of objects in a
   * sorted order.
   *
   * @param key       the dir key.
   * @param recursive indicate whether list dir recursively or not.
   * @return the sub objects of the given dir key, not include the dir key.
   */
  Iterable<ObjectInfo> listDir(String key, boolean recursive);

  /**
   * The given key could be a file or a directory, if a directory, it can an empty or non-empty
   * directory.
   * Deleting a non-empty dir is only allowed if recursive is true.
   *
   * @param key       the dir key.
   * @param recursive indicate whether delete dir recursively or not.
   */
  void deleteDir(String key, boolean recursive);

  /**
   * Whether the directory is empty.
   *
   * @param key the dir key.
   * @return true if the dir is empty or doesn't exist.
   */
  boolean isEmptyDir(String key);
}
