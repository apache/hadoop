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

import org.apache.hadoop.fs.swift.auth.Roles;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;

/**
 * Describes user entity in Keystone
 * In different Swift installations User is represented differently.
 * To avoid any JSON deserialization failures this entity is ignored.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

  /**
   * user id in Keystone
   */
  private String id;

  /**
   * user human readable name
   */
  private String name;

  /**
   * user roles in Keystone
   */
  private List<Roles> roles;

  /**
   * links to user roles
   */
  private List<Object> roles_links;

  /**
   * human readable username in Keystone
   */
  private String username;

  /**
   * @return user id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id user id
   */
  public void setId(String id) {
    this.id = id;
  }


  /**
   * @return user name
   */
  public String getName() {
    return name;
  }


  /**
   * @param name user name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return user roles
   */
  public List<Roles> getRoles() {
    return roles;
  }

  /**
   * @param roles sets user roles
   */
  public void setRoles(List<Roles> roles) {
    this.roles = roles;
  }

  /**
   * @return user roles links
   */
  public List<Object> getRoles_links() {
    return roles_links;
  }

  /**
   * @param roles_links user roles links
   */
  public void setRoles_links(List<Object> roles_links) {
    this.roles_links = roles_links;
  }

  /**
   * @return username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username human readable user name
   */
  public void setUsername(String username) {
    this.username = username;
  }
}
