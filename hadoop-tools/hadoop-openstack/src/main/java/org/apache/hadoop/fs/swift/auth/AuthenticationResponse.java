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

import org.apache.hadoop.fs.swift.auth.entities.AccessToken;
import org.apache.hadoop.fs.swift.auth.entities.Catalog;
import org.apache.hadoop.fs.swift.auth.entities.User;

import java.util.List;

/**
 * Response from KeyStone deserialized into AuthenticationResponse class.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class AuthenticationResponse {
  private Object metadata;
  private List<Catalog> serviceCatalog;
  private User user;
  private AccessToken token;

  public Object getMetadata() {
    return metadata;
  }

  public void setMetadata(Object metadata) {
    this.metadata = metadata;
  }

  public List<Catalog> getServiceCatalog() {
    return serviceCatalog;
  }

  public void setServiceCatalog(List<Catalog> serviceCatalog) {
    this.serviceCatalog = serviceCatalog;
  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }

  public AccessToken getToken() {
    return token;
  }

  public void setToken(AccessToken token) {
    this.token = token;
  }
}
