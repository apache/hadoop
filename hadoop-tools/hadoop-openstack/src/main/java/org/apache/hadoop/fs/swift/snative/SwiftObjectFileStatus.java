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

package org.apache.hadoop.fs.swift.snative;

import java.util.Date;

/**
 * Java mapping of Swift JSON file status.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */

class SwiftObjectFileStatus {
  private long bytes;
  private String content_type;
  private String hash;
  private Date last_modified;
  private String name;
  private String subdir;

  SwiftObjectFileStatus() {
  }

  SwiftObjectFileStatus(long bytes, String content_type, String hash,
                        Date last_modified, String name) {
    this.bytes = bytes;
    this.content_type = content_type;
    this.hash = hash;
    this.last_modified = last_modified;
    this.name = name;
  }

  public long getBytes() {
    return bytes;
  }

  public void setBytes(long bytes) {
    this.bytes = bytes;
  }

  public String getContent_type() {
    return content_type;
  }

  public void setContent_type(String content_type) {
    this.content_type = content_type;
  }

  public String getHash() {
    return hash;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public Date getLast_modified() {
    return last_modified;
  }

  public void setLast_modified(Date last_modified) {
    this.last_modified = last_modified;
  }

  public String getName() {
    return pathToRootPath(name);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSubdir() {
    return pathToRootPath(subdir);
  }

  public void setSubdir(String subdir) {
    this.subdir = subdir;
  }

  /**
   * If path doesn't starts with '/'
   * method will concat '/'
   *
   * @param path specified path
   * @return root path string
   */
  private String pathToRootPath(String path) {
    if (path == null) {
      return null;
    }

    if (path.startsWith("/")) {
      return path;
    }

    return "/".concat(path);
  }
}
