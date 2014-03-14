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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.cache.LoadingCache;

public class LocalizerContext {

  private final String user;
  private final ContainerId containerId;
  private final Credentials credentials;
  private final LoadingCache<Path,Future<FileStatus>> statCache;

  public LocalizerContext(String user, ContainerId containerId,
      Credentials credentials) {
    this(user, containerId, credentials, null);
  }

  public LocalizerContext(String user, ContainerId containerId,
      Credentials credentials,
      LoadingCache<Path,Future<FileStatus>> statCache) {
    this.user = user;
    this.containerId = containerId;
    this.credentials = credentials;
    this.statCache = statCache;
  }

  public String getUser() {
    return user;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public LoadingCache<Path,Future<FileStatus>> getStatCache() {
    return statCache;
  }
}
