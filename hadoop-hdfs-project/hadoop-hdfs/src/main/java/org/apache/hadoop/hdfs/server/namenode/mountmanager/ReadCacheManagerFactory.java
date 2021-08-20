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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

/**
 * Factory for the {@link SimpleReadCacheManager}.
 */
@InterfaceAudience.Private
public class ReadCacheManagerFactory
    extends ProvidedReadCacheManagerSpi.Factory<SimpleReadCacheManager> {

  @Override
  public SimpleReadCacheManager newInstance(Configuration conf,
      FSNamesystem namesystem, BlockManager blockManager) {
    return new SimpleReadCacheManager(conf, namesystem, blockManager);
  }
}
