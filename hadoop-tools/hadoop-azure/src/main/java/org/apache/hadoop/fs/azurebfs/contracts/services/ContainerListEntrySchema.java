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
import java.util.Objects;

/**
 * List result entry schema for the Azure Blob Storage List Containers API.
 *
 * Represents a single container and its associated properties returned by
 * the Blob endpoint container listing operation.
 */
public class ContainerListEntrySchema implements ListResultEntrySchema {

  private String name;
  private String version;
  private Boolean deleted = false;
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

  private final Map<String, String> metadata = new HashMap<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * {@inheritDoc}
   *
   * Containers are treated as directories in ABFS semantics.
   */
  @Override
  public Boolean isDirectory() {
    return Boolean.TRUE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String eTag() {
    return eTag;
  }

  /**
   * {@inheritDoc}
   *
   * Returns the container last-modified time as a string, if available.
   */
  @Override
  public String lastModified() {
    return Objects.toString(lastModified, null);
  }

  /**
   * {@inheritDoc}
   *
   * Containers do not have a content length.
   */
  @Override
  public Long contentLength() {
    return 0L;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String owner() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String group() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String permissions() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getXMsEncryptionContext() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCustomerProvidedKeySha256() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListResultEntrySchema withName(final String name) {
    this.name = name;
    return this;
  }

  /** @return container name */
  public String getName() {
    return name;
  }

  /** @return container version */
  public String getVersion() {
    return version;
  }

  /** @return true if the container is marked as deleted */
  public Boolean getDeleted() {
    return deleted;
  }

  /** @return last modified time */
  public Long getLastModified() {
    return lastModified;
  }

  /** @return lease status */
  public String getLeaseStatus() {
    return leaseStatus;
  }

  /** @return lease state */
  public String getLeaseState() {
    return leaseState;
  }

  /** @return lease duration */
  public String getLeaseDuration() {
    return leaseDuration;
  }

  /** @return public access level */
  public String getPublicAccess() {
    return publicAccess;
  }

  /** @return immutability policy flag */
  public Boolean getHasImmutabilityPolicy() {
    return hasImmutabilityPolicy;
  }

  /** @return legal hold flag */
  public Boolean getHasLegalHold() {
    return hasLegalHold;
  }

  /** @return deleted time */
  public Long getDeletedTime() {
    return deletedTime;
  }

  /** @return remaining retention days */
  public Integer getRemainingRetentionDays() {
    return remainingRetentionDays;
  }

  /**
   * Returns user-defined metadata associated with the container.
   *
   * @return container metadata key-value pairs
   */
  public Map<String, String> metadata() {
    return metadata;
  }

  /** @param name container name */
  public void setName(final String name) {
    this.name = name;
  }

  /** @param version container version */
  public void setVersion(final String version) {
    this.version = version;
  }

  /** @param deleted deleted flag */
  public void setDeleted(final Boolean deleted) {
    this.deleted = deleted;
  }

  /** @param lastModified last modified time */
  public void setLastModified(final Long lastModified) {
    this.lastModified = lastModified;
  }

  /** @param eTag entity tag */
  public void setETag(final String eTag) {
    this.eTag = eTag;
  }

  /** @param leaseStatus lease status */
  public void setLeaseStatus(final String leaseStatus) {
    this.leaseStatus = leaseStatus;
  }

  /** @param leaseState lease state */
  public void setLeaseState(final String leaseState) {
    this.leaseState = leaseState;
  }

  /** @param leaseDuration lease duration */
  public void setLeaseDuration(final String leaseDuration) {
    this.leaseDuration = leaseDuration;
  }

  /** @param publicAccess public access level */
  public void setPublicAccess(final String publicAccess) {
    this.publicAccess = publicAccess;
  }

  /** @param hasImmutabilityPolicy immutability policy flag */
  public void setHasImmutabilityPolicy(final Boolean hasImmutabilityPolicy) {
    this.hasImmutabilityPolicy = hasImmutabilityPolicy;
  }

  /** @param hasLegalHold legal hold flag */
  public void setHasLegalHold(final Boolean hasLegalHold) {
    this.hasLegalHold = hasLegalHold;
  }

  /** @param deletedTime deletion time */
  public void setDeletedTime(final Long deletedTime) {
    this.deletedTime = deletedTime;
  }

  /** @param remainingRetentionDays remaining retention days */
  public void setRemainingRetentionDays(final Integer remainingRetentionDays) {
    this.remainingRetentionDays = remainingRetentionDays;
  }

  /**
   * Adds a metadata key-value pair for the container.
   *
   * @param key metadata key
   * @param value metadata value
   */
  public void addMetadata(final String key, final String value) {
    metadata.put(key, value);
  }
}
