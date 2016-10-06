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

package org.apache.hadoop.fs.swift.auth.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.net.URI;

/**
 * Openstack Swift endpoint description.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class Endpoint {

  /**
   * endpoint id
   */
  private String id;

  /**
   * Keystone admin URL
   */
  private URI adminURL;

  /**
   * Keystone internal URL
   */
  private URI internalURL;

  /**
   * public accessible URL
   */
  private URI publicURL;

  /**
   * public accessible URL#2
   */
  private URI publicURL2;

  /**
   * Openstack region name
   */
  private String region;

  /**
   * This field is used in RackSpace authentication model
   */
  private String tenantId;

  /**
   * This field user in RackSpace auth model
   */
  private String versionId;

  /**
   * This field user in RackSpace auth model
   */
  private String versionInfo;

  /**
   * This field user in RackSpace auth model
   */
  private String versionList;


  /**
   * @return endpoint id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id endpoint id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return Keystone admin URL
   */
  public URI getAdminURL() {
    return adminURL;
  }

  /**
   * @param adminURL Keystone admin URL
   */
  public void setAdminURL(URI adminURL) {
    this.adminURL = adminURL;
  }

  /**
   * @return internal Keystone
   */
  public URI getInternalURL() {
    return internalURL;
  }

  /**
   * @param internalURL Keystone internal URL
   */
  public void setInternalURL(URI internalURL) {
    this.internalURL = internalURL;
  }

  /**
   * @return public accessible URL
   */
  public URI getPublicURL() {
    return publicURL;
  }

  /**
   * @param publicURL public URL
   */
  public void setPublicURL(URI publicURL) {
    this.publicURL = publicURL;
  }

  public URI getPublicURL2() {
    return publicURL2;
  }

  public void setPublicURL2(URI publicURL2) {
    this.publicURL2 = publicURL2;
  }

  /**
   * @return Openstack region name
   */
  public String getRegion() {
    return region;
  }

  /**
   * @param region Openstack region name
   */
  public void setRegion(String region) {
    this.region = region;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getVersionId() {
    return versionId;
  }

  public void setVersionId(String versionId) {
    this.versionId = versionId;
  }

  public String getVersionInfo() {
    return versionInfo;
  }

  public void setVersionInfo(String versionInfo) {
    this.versionInfo = versionInfo;
  }

  public String getVersionList() {
    return versionList;
  }

  public void setVersionList(String versionList) {
    this.versionList = versionList;
  }
}
