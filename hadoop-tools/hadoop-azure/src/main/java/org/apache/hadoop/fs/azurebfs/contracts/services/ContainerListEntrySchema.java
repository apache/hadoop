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

/**
 * List Result Entry Schema for Blob Endpoint List Containers API.
 *
 * <p>
 * Represents a single container returned by the Azure Blob Storage
 * List Containers REST API.
 * </p>
 */
public class ContainerListEntrySchema implements ListResultEntrySchema {

  /* ================= Container fields ================= */

  private String name;
  private String version;
  private Boolean deleted = false;

  /* ================= Properties ================= */

  private Long lastModified;
  private String eTag;
  private String leaseStatus;
  private String leaseState;
  private String leaseDuration;
  private String publicAccess;
  private Boolean hasImmutabilityPolicy;
  private Boolean hasLegalHold;
  private Long deletedTime;
  private Integer remainingRetentionDays;

  /* ================= Metadata ================= */

  private final Map<String, String> metadata = new HashMap<>();

  /* ================= Interface methods ================= */

  @Override
  public String name() {
    return name;
  }

  /**
   * Containers are never directories in ABFS semantics.
   */
  @Override
  public Boolean isDirectory() {
    return Boolean.TRUE;
  }

  @Override
  public String eTag() {
    return eTag;
  }

  @Override
  public String lastModified() {
    return lastModified != null ? String.valueOf(lastModified) : null;
  }

  @Override
  public Long contentLength() {
    // Containers do not have a content length
    return 0L;
  }

  @Override
  public String owner() {
    return null;
  }

  @Override
  public String group() {
    return null;
  }

  @Override
  public String permissions() {
    return null;
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

  /* ================= Getters ================= */

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public Boolean getDeleted() {
    return deleted;
  }

  public Long getLastModified() {
    return lastModified;
  }

  public String getLeaseStatus() {
    return leaseStatus;
  }

  public String getLeaseState() {
    return leaseState;
  }

  public String getLeaseDuration() {
    return leaseDuration;
  }

  public String getPublicAccess() {
    return publicAccess;
  }

  public Boolean getHasImmutabilityPolicy() {
    return hasImmutabilityPolicy;
  }

  public Boolean getHasLegalHold() {
    return hasLegalHold;
  }

  public Long getDeletedTime() {
    return deletedTime;
  }

  public Integer getRemainingRetentionDays() {
    return remainingRetentionDays;
  }

  public Map<String, String> metadata() {
    return metadata;
  }

  /* ================= Setters ================= */

  public void setName(final String name) {
    this.name = name;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  public void setDeleted(final Boolean deleted) {
    this.deleted = deleted;
  }

  public void setLastModified(final Long lastModified) {
    this.lastModified = lastModified;
  }

  public void setETag(final String eTag) {
    this.eTag = eTag;
  }

  public void setLeaseStatus(final String leaseStatus) {
    this.leaseStatus = leaseStatus;
  }

  public void setLeaseState(final String leaseState) {
    this.leaseState = leaseState;
  }

  public void setLeaseDuration(final String leaseDuration) {
    this.leaseDuration = leaseDuration;
  }

  public void setPublicAccess(final String publicAccess) {
    this.publicAccess = publicAccess;
  }

  public void setHasImmutabilityPolicy(final Boolean hasImmutabilityPolicy) {
    this.hasImmutabilityPolicy = hasImmutabilityPolicy;
  }

  public void setHasLegalHold(final Boolean hasLegalHold) {
    this.hasLegalHold = hasLegalHold;
  }

  public void setDeletedTime(final Long deletedTime) {
    this.deletedTime = deletedTime;
  }

  public void setRemainingRetentionDays(final Integer remainingRetentionDays) {
    this.remainingRetentionDays = remainingRetentionDays;
  }

  public void addMetadata(final String key, final String value) {
    metadata.put(key, value);
  }
}

