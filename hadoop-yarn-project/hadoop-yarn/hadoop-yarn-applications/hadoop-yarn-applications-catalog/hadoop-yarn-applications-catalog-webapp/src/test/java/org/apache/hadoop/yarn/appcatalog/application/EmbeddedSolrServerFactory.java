/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.appcatalog.application;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Embedded solr server factory class for unit tests.
 */
public final class EmbeddedSolrServerFactory {

  private EmbeddedSolrServerFactory() {
  }

  /**
   * Cleans the given solrHome directory and creates a new EmbeddedSolrServer.
   *
   * @param solrHome
   *          the Solr home directory to use
   * @param configSetHome
   *          the directory containing config sets
   * @param coreName
   *          the name of the core, must have a matching directory in configHome
   *
   * @return an EmbeddedSolrServer with a core created for the given coreName
   * @throws IOException
   */
  public static SolrClient create(final String solrHome,
      final String configSetHome, final String coreName)
      throws IOException, SolrServerException {
    return create(solrHome, configSetHome, coreName, true);
  }

  /**
   * @param solrHome
   *          the Solr home directory to use
   * @param configSetHome
   *          the directory containing config sets
   * @param coreName
   *          the name of the core, must have a matching directory in configHome
   * @param cleanSolrHome
   *          if true the directory for solrHome will be deleted and re-created
   *          if it already exists
   *
   * @return an EmbeddedSolrServer with a core created for the given coreName
   * @throws IOException
   */
  public static SolrClient create(final String solrHome,
      final String configSetHome, final String coreName,
      final boolean cleanSolrHome) throws IOException, SolrServerException {

    final File solrHomeDir = new File(solrHome);
    if (solrHomeDir.exists()) {
      FileUtils.deleteDirectory(solrHomeDir);
      solrHomeDir.mkdirs();
    } else {
      solrHomeDir.mkdirs();
    }

    final SolrResourceLoader loader = new SolrResourceLoader(
        solrHomeDir.toPath());
    final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

    final NodeConfig config = new NodeConfig.NodeConfigBuilder(
        "embeddedSolrServerNode", loader)
            .setConfigSetBaseDirectory(configSetPath.toString()).build();

    final EmbeddedSolrServer embeddedSolrServer = new EmbeddedSolrServer(config,
        coreName);

    final CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
    createRequest.setCoreName(coreName);
    createRequest.setConfigSet(coreName);
    embeddedSolrServer.request(createRequest);

    return embeddedSolrServer;
  }

}
