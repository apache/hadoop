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

package org.apache.hadoop.ozone.web.ozShell;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;

/**
 * Common interface for command handling.
 */
public abstract class Handler {

  protected static final Logger LOG = LoggerFactory.getLogger(Handler.class);
  protected OzoneClient client;

  /**
   * Executes the Client command.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  protected abstract void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException;

  /**
   * verifies user provided URI.
   *
   * @param uri - UriString
   * @return URI
   * @throws URISyntaxException
   * @throws OzoneException
   */
  protected URI verifyURI(String uri)
      throws URISyntaxException, OzoneException, IOException {
    if ((uri == null) || uri.isEmpty()) {
      throw new OzoneClientException(
          "Ozone URI is needed to execute this command.");
    }
    URIBuilder ozoneURI = new URIBuilder(uri);
    if (ozoneURI.getPort() == 0) {
      ozoneURI.setPort(Shell.DEFAULT_OZONE_PORT);
    }

    Configuration conf = new OzoneConfiguration();
    String scheme = ozoneURI.getScheme();
    if (scheme.equals(OZONE_HTTP_SCHEME)) {
      if (ozoneURI.getHost() != null) {
        if (ozoneURI.getPort() == -1) {
          client = OzoneClientFactory.getRestClient(ozoneURI.getHost());
        } else {
          client = OzoneClientFactory
              .getRestClient(ozoneURI.getHost(), ozoneURI.getPort(), conf);
        }
      } else {
        client = OzoneClientFactory.getRestClient(conf);
      }
    } else if (scheme.equals(OZONE_URI_SCHEME) || scheme.isEmpty()) {
      if (ozoneURI.getHost() != null) {
        if (ozoneURI.getPort() == -1) {
          client = OzoneClientFactory.getRpcClient(ozoneURI.getHost());
        } else {
          client = OzoneClientFactory
              .getRpcClient(ozoneURI.getHost(), ozoneURI.getPort(), conf);
        }
      } else {
        client = OzoneClientFactory.getRpcClient(conf);
      }
    } else {
      throw new OzoneClientException("Invalid URI: " + ozoneURI);
    }
    return ozoneURI.build();
  }
}
