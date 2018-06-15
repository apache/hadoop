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

package org.apache.hadoop.fs.azurebfs.services;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;

@Singleton
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AbfsHttpClientFactoryImpl implements AbfsHttpClientFactory {
  private final ConfigurationService configurationService;

  @Inject
  AbfsHttpClientFactoryImpl(
      final ConfigurationService configurationService) {

    Preconditions.checkNotNull(configurationService, "configurationService");

    this.configurationService = configurationService;
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, final FileSystem fs) {
    final AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;

    String scheme = FileSystemUriSchemes.HTTP_SCHEME;

    if (abfs.isSecure()) {
      scheme = FileSystemUriSchemes.HTTPS_SCHEME;
    }

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(hostName);

    return uriBuilder;
  }

  public AbfsClient create(final AzureBlobFileSystem fs) throws AzureBlobFileSystemException {
    final URI uri = fs.getUri();
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      final String errMsg = String
          .format("URI '%s' has a malformed authority, expected container name. "
                  + "Authority takes the form "+ FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
              uri.toString());
      throw new InvalidUriException(errMsg);
    }

    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];

    final URIBuilder uriBuilder = getURIBuilder(accountName, fs);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + fileSystemName;

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(String.format("URI '%s' is malformed", uri.toString()));
    }

    SharedKeyCredentials creds =
        new SharedKeyCredentials(accountName.substring(0, accountName.indexOf(AbfsHttpConstants.DOT)),
                this.configurationService.getStorageAccountKey(accountName));

    return new AbfsClient(baseUrl, creds, configurationService, new ExponentialRetryPolicy());
  }
}