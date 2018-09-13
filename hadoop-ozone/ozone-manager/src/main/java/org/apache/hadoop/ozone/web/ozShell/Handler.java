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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.rest.OzoneException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Common interface for command handling.
 */
@Command(mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public abstract class Handler implements Callable<Void> {

  protected static final Logger LOG = LoggerFactory.getLogger(Handler.class);

  protected OzoneClient client;

  @ParentCommand
  private GenericParentCommand parent;

  @Override
  public Void call() throws Exception {
    throw new UnsupportedOperationException();
  }

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
    URIBuilder ozoneURI = new URIBuilder(stringToUri(uri));
    if (ozoneURI.getPort() == 0) {
      ozoneURI.setPort(Shell.DEFAULT_OZONE_PORT);
    }

    Configuration conf = new OzoneConfiguration();
    String scheme = ozoneURI.getScheme();
    if (ozoneURI.getScheme() == null || scheme.isEmpty()) {
      scheme = OZONE_URI_SCHEME;
    }
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
    } else if (scheme.equals(OZONE_URI_SCHEME)) {
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

  /** Construct a URI from a String with unescaped special characters
   *  that have non-standard semantics. e.g. /, ?, #. A custom parsing
   *  is needed to prevent misbehavior.
   *  @param pathString The input path in string form
   *  @return URI
   */
  private static URI stringToUri(String pathString) throws IOException {
    // parse uri components
    String scheme = null;
    String authority = null;
    int start = 0;

    // parse uri scheme, if any
    int colon = pathString.indexOf(':');
    int slash = pathString.indexOf('/');
    if (colon > 0 && (slash == colon +1)) {
      // has a non zero-length scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length()-start > 2)) {
      start += 2;
      int nextSlash = pathString.indexOf('/', start);
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start, authEnd);
      start = authEnd;
    }
    // uri path is the rest of the string. ? or # are not interpreted,
    // but any occurrence of them will be quoted by the URI ctor.
    String path = pathString.substring(start, pathString.length());

    // Construct the URI
    try {
      return new URI(scheme, authority, path, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public boolean isVerbose() {
    return parent.isVerbose();
  }

}
