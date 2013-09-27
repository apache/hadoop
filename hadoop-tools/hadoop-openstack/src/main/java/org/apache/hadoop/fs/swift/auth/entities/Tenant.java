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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Tenant is abstraction in Openstack which describes all account
 * information and user privileges in system.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tenant {

  /**
   * tenant id
   */
  private String id;

  /**
   * tenant short description which Keystone returns
   */
  private String description;

  /**
   * boolean enabled user account or no
   */
  private boolean enabled;

  /**
   * tenant human readable name
   */
  private String name;

  /**
   * @return tenant name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name tenant name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return true if account enabled and false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled enable or disable
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * @return account short description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description set account description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return set tenant id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id tenant id
   */
  public void setId(String id) {
    this.id = id;
  }
}
