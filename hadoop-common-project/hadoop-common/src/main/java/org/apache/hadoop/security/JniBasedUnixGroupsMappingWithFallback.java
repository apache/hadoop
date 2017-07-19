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

package org.apache.hadoop.security;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniBasedUnixGroupsMappingWithFallback implements
    GroupMappingServiceProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(JniBasedUnixGroupsMappingWithFallback.class);
  
  private GroupMappingServiceProvider impl;

  public JniBasedUnixGroupsMappingWithFallback() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      this.impl = new JniBasedUnixGroupsMapping();
    } else {
      PerformanceAdvisory.LOG.debug("Falling back to shell based");
      this.impl = new ShellBasedUnixGroupsMapping();
    }
    if (LOG.isDebugEnabled()){
      LOG.debug("Group mapping impl=" + impl.getClass().getName());
    }
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    return impl.getGroups(user);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    impl.cacheGroupsRefresh();
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    impl.cacheGroupsAdd(groups);
  }

}
