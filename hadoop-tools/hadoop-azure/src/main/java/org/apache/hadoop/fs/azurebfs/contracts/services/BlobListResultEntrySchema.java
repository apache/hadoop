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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * List Result Entry Schema for Blob Endpoint List Blob API
 */
public class BlobListResultEntrySchema implements ListResultEntrySchema {

  private String name;
  private Path path;
  private String url;
  private Boolean isDirectory = false;
  private String eTag;
  private String lastModifiedTime;
  private String creationTime;
  private String owner;
  private String group;
  private String permission;
  private String acl;
  private Long contentLength = 0L;
  private String copyId;
  private String copyStatus;
  private String copySourceUrl;
  private String copyProgress;
  private String copyStatusDescription;
  private long copyCompletionTime;
  private Map<String, String> metadata = new HashMap<>();

  @Override
  public String name() {
    return name;
  }

  public Path path() {
    return path;
  }

  public String url() {
    return url;
  }

  @Override
  public Boolean isDirectory() {
    return isDirectory;
  }

  @Override
  public String eTag() {
    return eTag;
  }

  @Override
  public String lastModified() {
    return String.valueOf(lastModifiedTime);
  }

  public String creation() {
    return String.valueOf(lastModifiedTime);
  }

  public String lastModifiedTime() {
    return lastModifiedTime;
  }

  public String creationTime() {
    return creationTime;
  }

  @Override
  public Long contentLength() {
    return contentLength;
  }

  public String copyId() {
    return copyId;
  }

  public String copyStatus() {
    return copyStatus;
  }

  public String copySourceUrl() {
    return copySourceUrl;
  }

  public String copyProgress() {
    return copyProgress;
  }

  public String copyStatusDescription() {
    return copyStatusDescription;
  }

  public long copyCompletionTime() {
    return copyCompletionTime;
  }

  public Map<String, String> metadata() {
    return metadata;
  }

  @Override
  public String owner() {
    return owner;
  }

  @Override
  public String group() {
    return group;
  }

  @Override
  public String permissions() {
    return permission;
  }

  @Override
  public String getXMsEncryptionContext() {
    return null;
  }

  @Override
  public String getCustomerProvidedKeySha256() {
    return null;
  }

  @Override
  public ListResultEntrySchema withName(final String name) {
    this.name = name;
    return this;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setPath(final Path path) {
    this.path = path;
  }

  public void setUrl(final String url) {
    this.url = url;
  }

  public void setIsDirectory(final Boolean isDirectory) {
    this.isDirectory = isDirectory;
  }

  public void setETag(final String eTag) {
    this.eTag = eTag;
  }

  public void setLastModifiedTime(final String lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public void setCreationTime(final String creationTime) {
    this.creationTime = creationTime;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public void setGroup(final String group) {
    this.group = group;
  }

  public void setPermission(final String permission) {
    this.permission = permission;
  }

  public void setAcl(final String acl) {
    this.acl = acl;
  }

  public void setContentLength(final Long contentLength) {
    this.contentLength = contentLength;
  }

  public void setCopyId(final String copyId) {
    this.copyId = copyId;
  }

  public void setCopyStatus(final String copyStatus) {
    this.copyStatus = copyStatus;
  }

  public void setCopyProgress(final String copyProgress) {
    this.copyProgress = copyProgress;
  }

  public void setCopySourceUrl(final String copySourceUrl) {
    this.copySourceUrl = copySourceUrl;
  }

  public void setCopyStatusDescription(final String copyStatusDescription) {
    this.copyStatusDescription = copyStatusDescription;
  }

  public void setCopyCompletionTime(final long copyCompletionTime) {
    this.copyCompletionTime = copyCompletionTime;
  }

  public void setMetadata(final Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void addMetadata(final String key, final String value) {
    this.metadata.put(key, value);
  }

  public String getAcl() {
    return acl;
  }
}
