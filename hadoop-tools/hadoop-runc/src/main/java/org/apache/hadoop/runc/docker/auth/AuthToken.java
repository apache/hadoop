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

package org.apache.hadoop.runc.docker.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.hadoop.runc.docker.json.InstantDeserializer;
import org.apache.hadoop.runc.docker.json.InstantSerializer;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthToken {

  private String token;
  private int expiresIn = 60;
  private Instant issuedAt = Instant.now();

  @JsonProperty
  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  @JsonProperty("expires_in")
  public int getExpiresIn() {
    return expiresIn;
  }

  public void setExpiresIn(int expiresIn) {
    this.expiresIn = expiresIn;
  }

  @JsonProperty("issued_at")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  public Instant getIssuedAt() {
    return issuedAt;
  }

  public void setIssuedAt(Instant issuedAt) {
    this.issuedAt = issuedAt;
  }

  @JsonIgnore
  public boolean isExpired() {
    return issuedAt.plusSeconds(expiresIn).isBefore(Instant.now());
  }

  @Override
  public String toString() {
    return String.format(
        "auth-token { token=%s, expiresIn=%d, issuedAt=%s, expired=%s }",
        (token == null || token.isEmpty()) ? "(none)" : "*redacted*", expiresIn,
        issuedAt, isExpired());
  }

}
