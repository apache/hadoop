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

import java.util.ArrayList;
import java.util.List;

public class ContainerListResponseData {

  private final List<ContainerListEntrySchema> containers =
      new ArrayList<>();

  private String continuationToken;
  private String prefix;
  private String marker;
  private Integer maxResults;

  /* ================= Containers ================= */

  public void addContainer(ContainerListEntrySchema container) {
    containers.add(container);
  }

  public List<ContainerListEntrySchema> getContainers() {
    return containers;
  }

  /* ================= Continuation ================= */

  public String getContinuationToken() {
    return continuationToken;
  }

  public void setContinuationToken(String continuationToken) {
    this.continuationToken = continuationToken;
  }

  /* ================= Optional enumeration fields ================= */

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public void setMarker(String marker) {
    this.marker = marker;
  }

  public void setMaxResults(Integer maxResults) {
    this.maxResults = maxResults;
  }
}
