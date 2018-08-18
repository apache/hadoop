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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.codehaus.jackson.annotate.JsonProperty;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * The ListResultEntrySchema model.
 */
@InterfaceStability.Evolving
public class ListResultEntrySchema {
  /**
   * The name property.
   */
  @JsonProperty(value = "name")
  private String name;

  /**
   * The isDirectory property.
   */
  @JsonProperty(value = "isDirectory")
  private Boolean isDirectory;

  /**
   * The lastModified property.
   */
  @JsonProperty(value = "lastModified")
  private String lastModified;

  /**
   * The eTag property.
   */
  @JsonProperty(value = "etag")
  private String eTag;

  /**
   * The contentLength property.
   */
  @JsonProperty(value = "contentLength")
  private Long contentLength;

  /**
   * The owner property.
   */
  @JsonProperty(value = "owner")
  private String owner;

  /**
   * The group property.
   */
  @JsonProperty(value = "group")
  private String group;

  /**
   * The permissions property.
   */
  @JsonProperty(value = "permissions")
  private String permissions;

  /**
   * Get the name value.
   *
   * @return the name value
   */
  public String name() {
    return name;
  }

  /**
   * Set the name value.
   *
   * @param name the name value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get the isDirectory value.
   *
   * @return the isDirectory value
   */
  public Boolean isDirectory() {
    return isDirectory;
  }

  /**
   * Set the isDirectory value.
   *
   * @param isDirectory the isDirectory value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withIsDirectory(final Boolean isDirectory) {
    this.isDirectory = isDirectory;
    return this;
  }

  /**
   * Get the lastModified value.
   *
   * @return the lastModified value
   */
  public String lastModified() {
    return lastModified;
  }

  /**
   * Set the lastModified value.
   *
   * @param lastModified the lastModified value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withLastModified(String lastModified) {
    this.lastModified = lastModified;
    return this;
  }

  /**
   * Get the etag value.
   *
   * @return the etag value
   */
  public String eTag() {
    return eTag;
  }

  /**
   * Set the eTag value.
   *
   * @param eTag the eTag value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withETag(final String eTag) {
    this.eTag = eTag;
    return this;
  }

  /**
   * Get the contentLength value.
   *
   * @return the contentLength value
   */
  public Long contentLength() {
    return contentLength;
  }

  /**
   * Set the contentLength value.
   *
   * @param contentLength the contentLength value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withContentLength(final Long contentLength) {
    this.contentLength = contentLength;
    return this;
  }

  /**
   *
   Get the owner value.
   *
   * @return the owner value
   */
  public String owner() {
    return owner;
  }

  /**
   * Set the owner value.
   *
   * @param owner the owner value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withOwner(final String owner) {
    this.owner = owner;
    return this;
  }

  /**
   * Get the group value.
   *
   * @return the group value
   */
  public String group() {
    return group;
  }

  /**
   * Set the group value.
   *
   * @param group the group value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withGroup(final String group) {
    this.group = group;
    return this;
  }

  /**
   * Get the permissions value.
   *
   * @return the permissions value
   */
  public String permissions() {
    return permissions;
  }

  /**
   * Set the permissions value.
   *
   * @param permissions the permissions value to set
   * @return the ListEntrySchema object itself.
   */
  public ListResultEntrySchema withPermissions(final String permissions) {
    this.permissions = permissions;
    return this;
  }

}