/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.XceiverClientRatis;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.rpc.RpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Helpers for Ratis tests.
 */
public interface RatisTestHelper {
  Logger LOG = LoggerFactory.getLogger(RatisTestHelper.class);

  static void initRatisConf(
      RpcType rpc, Pipeline pipeline, Configuration conf) {
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY, rpc.name());
    LOG.info(OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY
        + " = " + rpc.name());
    final String s = pipeline.getMachines().stream()
            .map(dn -> dn.getXferAddr())
            .collect(Collectors.joining(","));
    conf.setStrings(OzoneConfigKeys.DFS_CONTAINER_RATIS_CONF, s);
    LOG.info(OzoneConfigKeys.DFS_CONTAINER_RATIS_CONF + " = " + s);
  }

  static XceiverClientRatis newXceiverClientRatis(
      RpcType rpcType, Pipeline pipeline, OzoneConfiguration conf)
      throws IOException {
    initRatisConf(rpcType, pipeline, conf);
    return XceiverClientRatis.newXceiverClientRatis(pipeline, conf);
  }
}
