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
package org.apache.hadoop.hdfs.server.datanode.web;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Regression test for HDFS-16842.
 *
 * Verifies that {@link DatanodeHttpServer#updateConf(Configuration)} correctly
 * propagates a refreshed configuration so that WebHDFS connections made after
 * {@code DataNode#refreshNamenodes} can reach newly-added nameservices.
 */
public class TestDatanodeHttpServerUpdateConf {

  /**
   * After calling {@link DatanodeHttpServer#updateConf}, the internal
   * {@code conf} and {@code confForCreate} fields must point to the new
   * configuration, and the embedded Jetty info-server's servlet context
   * attributes must be updated accordingly.
   */
  @Test
  public void testUpdateConfRefreshesFields() throws Exception {
    Configuration original = new Configuration();
    original.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTP_ONLY.name());
    original.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "localhost:0");
    original.set("test.marker.key", "original");

    DatanodeHttpServer server = new DatanodeHttpServer(original, null, null);
    try {
      server.start();

      // Access internal fields via reflection.
      Field confField =
          DatanodeHttpServer.class.getDeclaredField("conf");
      confField.setAccessible(true);
      Field confForCreateField =
          DatanodeHttpServer.class.getDeclaredField("confForCreate");
      confForCreateField.setAccessible(true);
      Field infoServerField =
          DatanodeHttpServer.class.getDeclaredField("infoServer");
      infoServerField.setAccessible(true);
      HttpServer2 infoServer = (HttpServer2) infoServerField.get(server);

      // Pre-condition: conf field holds the original configuration.
      assertSame(original, confField.get(server),
          "conf field should initially be the original configuration");

      // Build the updated configuration (simulates refreshNamenodes loading
      // a new nameservice into a fresh Configuration).
      Configuration updated = new Configuration(original);
      updated.set("test.marker.key", "updated");
      updated.set("dfs.nameservices", "ns1,ns2");

      server.updateConf(updated);

      // After updateConf, the 'conf' field must point to the updated object.
      assertSame(updated, confField.get(server),
          "conf field must be the updated configuration after updateConf()");

      // confForCreate must incorporate the updated values + forced umask "000".
      Configuration updatedForCreate =
          (Configuration) confForCreateField.get(server);
      assertEquals("updated", updatedForCreate.get("test.marker.key"),
          "confForCreate must reflect updated config values");
      assertEquals("000", updatedForCreate.get(FsPermission.UMASK_LABEL),
          "confForCreate must enforce umask 000");

      // The Jetty info-server servlet context attributes must also be updated.
      assertSame(updated,
          infoServer.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE),
          "infoServer CONF_CONTEXT_ATTRIBUTE must be updated");
      assertSame(updated,
          infoServer.getAttribute(JspHelper.CURRENT_CONF),
          "infoServer CURRENT_CONF attribute must be updated");
    } finally {
      server.close();
    }
  }
}
