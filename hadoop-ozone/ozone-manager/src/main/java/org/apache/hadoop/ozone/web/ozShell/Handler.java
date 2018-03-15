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
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.ozone.web.client.OzoneRestClientException;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Common interface for command handling.
 */
public abstract class Handler {

  protected OzoneRestClient client;

  /**
   * Constructs a client object.
   */
  public Handler() {
    client = new OzoneRestClient();
  }

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
  protected URI verifyURI(String uri) throws URISyntaxException,
      OzoneException {
    if ((uri == null) || uri.isEmpty()) {
      throw new OzoneRestClientException(
          "Ozone URI is needed to execute this command.");
    }
    URIBuilder ozoneURI = new URIBuilder(uri);

    if (ozoneURI.getPort() == 0) {
      ozoneURI.setPort(Shell.DEFAULT_OZONE_PORT);
    }
    return ozoneURI.build();
  }


}
