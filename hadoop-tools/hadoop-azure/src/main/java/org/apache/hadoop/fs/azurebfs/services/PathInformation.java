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

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Information about a path.
 */
public class PathInformation {

  private Boolean pathExists;

  private Boolean isDirectory;

  private Boolean isImplicit;

  private String eTag;

  /**
   * Constructor.
   * @param pathExists The path exists.
   * @param isDirectory Is the path a directory?
   * @param eTag The ETag of the path.
   * @param isImplicit Is the path implicit?
   */
  public PathInformation(Boolean pathExists,
      Boolean isDirectory,
      String eTag,
      Boolean isImplicit) {
    this.pathExists = pathExists;
    this.isDirectory = isDirectory;
    this.eTag = eTag;
    this.isImplicit = isImplicit;
  }

  public PathInformation() {
  }

  /**
   * Copy the path information.
   * @param pathInformation The path information to copy.
   */
  public void copy(PathInformation pathInformation) {
    this.pathExists = pathInformation.getPathExists();
    this.isDirectory = pathInformation.getIsDirectory();
    this.eTag = pathInformation.getETag();
    this.isImplicit = pathInformation.getIsImplicit();
  }

  /**
   * Get the ETag of the path.
   *
   * @return the etag value*/
  public String getETag() {
    return eTag;
  }

  /**
   * Get value of pathExists.
   *
   * @return true if path exists, false otherwise.
   */
  public Boolean getPathExists() {
    return pathExists;
  }

  /**
   * Get value of isDirectory.
   *
   * @return true if path is a directory, false otherwise.
   */
  public Boolean getIsDirectory() {
    return isDirectory;
  }

  /**
   * Get value of isImplicit.
   *
   * @return true if path is implicit, false otherwise.
   */
  public Boolean getIsImplicit() {
    return isImplicit;
  }

  /**
   * Set the eTag value.
   *
   * @param eTag The eTag value to set.
   */
  void setETag(String eTag) {
    this.eTag = eTag;
  }
}
