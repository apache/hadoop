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

package org.apache.hadoop.fs.swift.auth;


/**
 * Describes credentials to log in Swift using Keystone authentication.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class PasswordCredentials {
  /**
   * user login
   */
  private String username;

  /**
   * user password
   */
  private String password;

  /**
   * default constructor
   */
  public PasswordCredentials() {
  }

  /**
   * @param username user login
   * @param password user password
   */
  public PasswordCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * @return user password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password user password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return login
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username login
   */
  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public String toString() {
    return "user '" + username + '\'' +
            " with password of length " + ((password == null) ? 0 : password.length());
  }
}

