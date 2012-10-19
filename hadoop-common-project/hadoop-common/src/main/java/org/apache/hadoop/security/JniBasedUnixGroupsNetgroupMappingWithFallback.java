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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

public class JniBasedUnixGroupsNetgroupMappingWithFallback implements
    GroupMappingServiceProvider {

  private static final Log LOG = LogFactory
      .getLog(JniBasedUnixGroupsNetgroupMappingWithFallback.class);

  private GroupMappingServiceProvider impl;

  public JniBasedUnixGroupsNetgroupMappingWithFallback() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      this.impl = new JniBasedUnixGroupsNetgroupMapping();
    } else {
      LOG.info("Falling back to shell based");
      this.impl = new ShellBasedUnixGroupsNetgroupMapping();
    }
    if (LOG.isDebugEnabled()) {
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
