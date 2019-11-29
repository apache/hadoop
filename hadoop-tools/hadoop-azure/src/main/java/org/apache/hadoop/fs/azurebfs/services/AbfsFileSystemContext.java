package org.apache.hadoop.fs.azurebfs.services;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.authentication.AbfsStoreAuthenticator;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizer;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ABFS_ENDPOINT;

public class AbfsFileSystemContext {

  public URI getUri() {
    return uri;
  }

  private final URI uri;

  public AbfsConfiguration getAbfsConfiguration() {
    return abfsConfiguration;
  }

  private final AbfsConfiguration abfsConfiguration;

  public AbfsStoreAuthenticator getAbfsStoreAuthenticator() {
    return abfsStoreAuthenticator;
  }

  private AbfsStoreAuthenticator abfsStoreAuthenticator;

  public String getFileSystemName() {
    return fileSystemName;
  }

  private final String fileSystemName;

  public String getAccountName() {
    return accountName;
  }

  private final String accountName;
  private URL baseUrl;

  public boolean isHttpsEnabled() {
    return httpsEnabled;
  }

  private final boolean httpsEnabled;

  public AbfsPerfTracker getAbfsPerfTracker() {
    return abfsPerfTracker;
  }

  private final AbfsPerfTracker abfsPerfTracker;

  public AbfsAuthorizer getAuthorizer() {
    return authorizer;
  }

  private final AbfsAuthorizer authorizer;

  public AbfsFileSystemContext(URI uri, Configuration configuration)
      throws IOException {
    this.uri = uri;

    String[] authorityParts = authorityParts(uri);
    this.fileSystemName = authorityParts[0];
    this.accountName = authorityParts[1];

    try {
      this.abfsConfiguration = new AbfsConfiguration(configuration,
          accountName);
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }

    this.abfsStoreAuthenticator = new AbfsStoreAuthenticator(
        this.abfsConfiguration, this.accountName, this.uri);

    this.httpsEnabled = (
        (this.abfsStoreAuthenticator.getAuthType() == AuthType.OAuth)
            || abfsConfiguration.isHttpsAlwaysUsed()) ?
        true :
        false; //TODO: was isSecureScheme() in AbfsFS - is it needed ?

    this.abfsPerfTracker = new AbfsPerfTracker(this.fileSystemName,
        this.accountName,
        this.abfsConfiguration);

    this.authorizer = this.abfsConfiguration.getAbfsAuthorizer();

    final URIBuilder uriBuilder =
        getURIBuilder(getAccountName(),
            isHttpsEnabled());

    final String url =
        uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + getFileSystemName();

    try {
      this.baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(uri.toString());
    }
  }

  public AuthType getAuthType()
  {
    return this.abfsStoreAuthenticator.getAuthType();
  }

  public URL getBaseUrl() throws InvalidUriException {


    return this.baseUrl;

  }

  private String[] authorityParts(URI uri) throws
      InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
          .format("'%s' has a malformed authority, expected container name. "
                  + "Authority takes the form "
                  + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
              uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);

    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfiguration.get(AZURE_ABFS_ENDPOINT);
    if (endPoint == null || !endPoint.contains(AbfsHttpConstants.COLON)) {
      uriBuilder.setHost(hostName);
      return uriBuilder;
    }

    // Split ip and port
    String[] data = endPoint.split(AbfsHttpConstants.COLON);
    if (data.length != 2) {
      throw new RuntimeException(String.format("ABFS endpoint is not set correctly : %s, "
          + "Do not specify scheme when using {IP}:{PORT}", endPoint));
    }
    uriBuilder.setHost(data[0].trim());
    uriBuilder.setPort(Integer.parseInt(data[1].trim()));
    uriBuilder.setPath("/" + UriUtils.extractAccountNameFromHostName(hostName));

    return uriBuilder;
  }
}
