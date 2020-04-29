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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS;

/**
 * Tests Router federation utility methods.
 */
public class TestFederationUtil {

  @Test
  public void testInstanceCreation() {
    Configuration conf = new HdfsConfiguration();

    // Use mock resolver classes
    conf.setClass(FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, ActiveNamenodeResolver.class);
    conf.setClass(FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, FileSubclusterResolver.class);

    Router router = new Router();
    StateStoreService stateStore = new StateStoreService();

    ActiveNamenodeResolver namenodeResolverWithContext =
        FederationUtil.newActiveNamenodeResolver(conf, stateStore);

    ActiveNamenodeResolver namenodeResolverWithoutContext =
        FederationUtil.newActiveNamenodeResolver(conf, null);

    FileSubclusterResolver subclusterResolverWithContext =
        FederationUtil.newFileSubclusterResolver(conf, router);

    FileSubclusterResolver subclusterResolverWithoutContext =
        FederationUtil.newFileSubclusterResolver(conf, null);

    // Check all instances are created successfully.
    assertNotNull(namenodeResolverWithContext);
    assertNotNull(namenodeResolverWithoutContext);
    assertNotNull(subclusterResolverWithContext);
    assertNotNull(subclusterResolverWithoutContext);
  }
}
