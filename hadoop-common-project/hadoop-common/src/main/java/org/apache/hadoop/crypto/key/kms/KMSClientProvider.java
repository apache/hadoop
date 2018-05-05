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
package org.apache.hadoop.crypto.key.kms;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.hadoop.util.KMSUtil;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import static org.apache.hadoop.util.KMSUtil.checkNotEmpty;
import static org.apache.hadoop.util.KMSUtil.checkNotNull;
import static org.apache.hadoop.util.KMSUtil.parseJSONEncKeyVersion;
import static org.apache.hadoop.util.KMSUtil.parseJSONEncKeyVersions;
import static org.apache.hadoop.util.KMSUtil.parseJSONKeyVersion;
import static org.apache.hadoop.util.KMSUtil.parseJSONMetadata;

/**
 * KMS client <code>KeyProvider</code> implementation.
 */
@InterfaceAudience.Private
public class KMSClientProvider extends KeyProvider implements CryptoExtension,
    KeyProviderDelegationTokenExtension.DelegationTokenExtension {

  private static final Logger LOG =
      LoggerFactory.getLogger(KMSClientProvider.class);

  private static final String INVALID_SIGNATURE = "Invalid signature";

  private static final String ANONYMOUS_REQUESTS_DISALLOWED = "Anonymous requests are disallowed";

  public static final String TOKEN_KIND_STR = KMSDelegationToken.TOKEN_KIND_STR;
  public static final Text TOKEN_KIND = KMSDelegationToken.TOKEN_KIND;

  public static final String SCHEME_NAME = "kms";

  private static final String UTF8 = "UTF-8";

  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON_MIME = "application/json";

  private static final String HTTP_GET = "GET";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_PUT = "PUT";
  private static final String HTTP_DELETE = "DELETE";


  private static final String CONFIG_PREFIX = "hadoop.security.kms.client.";

  /* Number of times to retry authentication in the event of auth failure
   * (normally happens due to stale authToken) 
   */
  public static final String AUTH_RETRY = CONFIG_PREFIX
      + "authentication.retry-count";
  public static final int DEFAULT_AUTH_RETRY = 1;

  private final ValueQueue<EncryptedKeyVersion> encKeyVersionQueue;

  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();

  private final Text dtService;

  // Allow fallback to default kms server port 9600 for certain tests that do
  // not specify the port explicitly in the kms provider url.
  @VisibleForTesting
  public static volatile boolean fallbackDefaultPortForTesting = false;

  private class EncryptedQueueRefiller implements
    ValueQueue.QueueRefiller<EncryptedKeyVersion> {

    @Override
    public void fillQueueForKey(String keyName,
        Queue<EncryptedKeyVersion> keyQueue, int numEKVs) throws IOException {
      checkNotNull(keyName, "keyName");
      Map<String, String> params = new HashMap<String, String>();
      params.put(KMSRESTConstants.EEK_OP, KMSRESTConstants.EEK_GENERATE);
      params.put(KMSRESTConstants.EEK_NUM_KEYS, "" + numEKVs);
      URL url = createURL(KMSRESTConstants.KEY_RESOURCE, keyName,
          KMSRESTConstants.EEK_SUB_RESOURCE, params);
      HttpURLConnection conn = createConnection(url, HTTP_GET);
      conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
      List response = call(conn, null,
          HttpURLConnection.HTTP_OK, List.class);
      List<EncryptedKeyVersion> ekvs =
          parseJSONEncKeyVersions(keyName, response);
      keyQueue.addAll(ekvs);
    }
  }

  /**
   * The KMS implementation of {@link TokenRenewer}.
   */
  public static class KMSTokenRenewer extends TokenRenewer {
    private static final Logger LOG =
        LoggerFactory.getLogger(KMSTokenRenewer.class);

    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(TOKEN_KIND);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException {
      LOG.debug("Renewing delegation token {}", token);
      KeyProvider keyProvider = KMSUtil.createKeyProvider(conf,
          KeyProviderFactory.KEY_PROVIDER_PATH);
      try {
        if (!(keyProvider instanceof
            KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
          throw new IOException(String
              .format("keyProvider %s cannot renew token [%s]",
                  keyProvider == null ? "null" : keyProvider.getClass(),
                  token));
        }
        return ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
            keyProvider).renewDelegationToken(token);
      } finally {
        if (keyProvider != null) {
          keyProvider.close();
        }
      }
    }

    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException {
      LOG.debug("Canceling delegation token {}", token);
      KeyProvider keyProvider = KMSUtil.createKeyProvider(conf,
          KeyProviderFactory.KEY_PROVIDER_PATH);
      try {
        if (!(keyProvider instanceof
            KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
          throw new IOException(String
              .format("keyProvider %s cannot cancel token [%s]",
                  keyProvider == null ? "null" : keyProvider.getClass(),
                  token));
        }
        ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
            keyProvider).cancelDelegationToken(token);
      } finally {
        if (keyProvider != null) {
          keyProvider.close();
        }
      }
    }
  }

  public static class KMSEncryptedKeyVersion extends EncryptedKeyVersion {
    public KMSEncryptedKeyVersion(String keyName, String keyVersionName,
        byte[] iv, String encryptedVersionName, byte[] keyMaterial) {
      super(keyName, keyVersionName, iv, new KMSKeyVersion(null, 
          encryptedVersionName, keyMaterial));
    }
  }

  private static void writeJson(Object obj, OutputStream os)
      throws IOException {
    Writer writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
    WRITER.writeValue(writer, obj);
  }

  /**
   * The factory to create KMSClientProvider, which is used by the
   * ServiceLoader.
   */
  public static class Factory extends KeyProviderFactory {

    /**
     * This provider expects URIs in the following form :
     * kms://<PROTO>@<AUTHORITY>/<PATH>
     *
     * where :
     * - PROTO = http or https
     * - AUTHORITY = <HOSTS>[:<PORT>]
     * - HOSTS = <HOSTNAME>[;<HOSTS>]
     * - HOSTNAME = string
     * - PORT = integer
     *
     * This will always create a {@link LoadBalancingKMSClientProvider}
     * if the uri is correct.
     */
    @Override
    public KeyProvider createProvider(URI providerUri, Configuration conf)
        throws IOException {
      if (SCHEME_NAME.equals(providerUri.getScheme())) {
        URL origUrl = new URL(extractKMSPath(providerUri).toString());
        String authority = origUrl.getAuthority();
        // check for ';' which delimits the backup hosts
        if (Strings.isNullOrEmpty(authority)) {
          throw new IOException(
              "No valid authority in kms uri [" + origUrl + "]");
        }
        // Check if port is present in authority
        // In the current scheme, all hosts have to run on the same port
        int port = -1;
        String hostsPart = authority;
        if (authority.contains(":")) {
          String[] t = authority.split(":");
          try {
            port = Integer.parseInt(t[1]);
          } catch (Exception e) {
            throw new IOException(
                "Could not parse port in kms uri [" + origUrl + "]");
          }
          hostsPart = t[0];
        }
        return createProvider(conf, origUrl, port, hostsPart);
      }
      return null;
    }

    private KeyProvider createProvider(Configuration conf,
        URL origUrl, int port, String hostsPart) throws IOException {
      String[] hosts = hostsPart.split(";");
      KMSClientProvider[] providers = new KMSClientProvider[hosts.length];
      for (int i = 0; i < hosts.length; i++) {
        try {
          providers[i] =
              new KMSClientProvider(
                  new URI("kms", origUrl.getProtocol(), hosts[i], port,
                      origUrl.getPath(), null, null), conf);
        } catch (URISyntaxException e) {
          throw new IOException("Could not instantiate KMSProvider.", e);
        }
      }
      return new LoadBalancingKMSClientProvider(providers, conf);
    }
  }

  private URL kmsUrl;
  private SSLFactory sslFactory;
  private ConnectionConfigurator configurator;
  private DelegationTokenAuthenticatedURL.Token authToken;
  private final int authRetry;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("KMSClientProvider[");
    sb.append(kmsUrl).append("]");
    return sb.toString();
  }

  /**
   * This small class exists to set the timeout values for a connection
   */
  private static class TimeoutConnConfigurator
          implements ConnectionConfigurator {
    private ConnectionConfigurator cc;
    private int timeout;

    /**
     * Sets the timeout and wraps another connection configurator
     * @param timeout - will set both connect and read timeouts - in seconds
     * @param cc - another configurator to wrap - may be null
     */
    public TimeoutConnConfigurator(int timeout, ConnectionConfigurator cc) {
      this.timeout = timeout;
      this.cc = cc;
    }

    /**
     * Calls the wrapped configure() method, then sets timeouts
     * @param conn the {@link HttpURLConnection} instance to configure.
     * @return the connection
     * @throws IOException
     */
    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
            throws IOException {
      if (cc != null) {
        conn = cc.configure(conn);
      }
      conn.setConnectTimeout(timeout * 1000);  // conversion to milliseconds
      conn.setReadTimeout(timeout * 1000);
      return conn;
    }
  }

  public KMSClientProvider(URI uri, Configuration conf) throws IOException {
    super(conf);
    kmsUrl = createServiceURL(extractKMSPath(uri));
    int kmsPort = kmsUrl.getPort();
    if ((kmsPort == -1) && fallbackDefaultPortForTesting) {
      kmsPort = 9600;
    }

    InetSocketAddress addr = new InetSocketAddress(kmsUrl.getHost(), kmsPort);
    dtService = SecurityUtil.buildTokenService(addr);

    if ("https".equalsIgnoreCase(kmsUrl.getProtocol())) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
    }
    int timeout = conf.getInt(
            CommonConfigurationKeysPublic.KMS_CLIENT_TIMEOUT_SECONDS,
            CommonConfigurationKeysPublic.KMS_CLIENT_TIMEOUT_DEFAULT);
    authRetry = conf.getInt(AUTH_RETRY, DEFAULT_AUTH_RETRY);
    configurator = new TimeoutConnConfigurator(timeout, sslFactory);
    encKeyVersionQueue =
        new ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion>(
            conf.getInt(
                CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE,
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT),
            conf.getFloat(
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK,
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT),
            conf.getInt(
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT),
            conf.getInt(
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS,
                CommonConfigurationKeysPublic.
                    KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT),
            new EncryptedQueueRefiller());
    authToken = new DelegationTokenAuthenticatedURL.Token();
    LOG.info("KMSClientProvider for KMS url: {} delegation token service: {}" +
        " created.", kmsUrl, dtService);
  }

  private static Path extractKMSPath(URI uri) throws MalformedURLException, IOException {
    return ProviderUtils.unnestUri(uri);
  }

  private static URL createServiceURL(Path path) throws IOException {
    String str = new URL(path.toString()).toExternalForm();
    if (str.endsWith("/")) {
      str = str.substring(0, str.length() - 1);
    }
    return new URL(str + KMSRESTConstants.SERVICE_VERSION + "/");
  }

  private URL createURL(String collection, String resource, String subResource,
      Map<String, ?> parameters) throws IOException {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(kmsUrl);
      if (collection != null) {
        sb.append(collection);
        if (resource != null) {
          sb.append("/").append(URLEncoder.encode(resource, UTF8));
          if (subResource != null) {
            sb.append("/").append(subResource);
          }
        }
      }
      URIBuilder uriBuilder = new URIBuilder(sb.toString());
      if (parameters != null) {
        for (Map.Entry<String, ?> param : parameters.entrySet()) {
          Object value = param.getValue();
          if (value instanceof String) {
            uriBuilder.addParameter(param.getKey(), (String) value);
          } else {
            for (String s : (String[]) value) {
              uriBuilder.addParameter(param.getKey(), s);
            }
          }
        }
      }
      return uriBuilder.build().toURL();
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
  }

  private HttpURLConnection configureConnection(HttpURLConnection conn)
      throws IOException {
    if (sslFactory != null) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
      try {
        httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
    }
    return conn;
  }

  private HttpURLConnection createConnection(final URL url, String method)
      throws IOException {
    HttpURLConnection conn;
    try {
      final String doAsUser = getDoAsUser();
      conn = getActualUgi().doAs(new PrivilegedExceptionAction
          <HttpURLConnection>() {
        @Override
        public HttpURLConnection run() throws Exception {
          DelegationTokenAuthenticatedURL authUrl =
              new DelegationTokenAuthenticatedURL(configurator);
          return authUrl.openConnection(url, authToken, doAsUser);
        }
      });
    } catch (IOException ex) {
      if (ex instanceof SocketTimeoutException) {
        LOG.warn("Failed to connect to {}:{}", url.getHost(), url.getPort());
      }
      throw ex;
    } catch (UndeclaredThrowableException ex) {
      throw new IOException(ex.getUndeclaredThrowable());
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    conn.setUseCaches(false);
    conn.setRequestMethod(method);
    if (method.equals(HTTP_POST) || method.equals(HTTP_PUT)) {
      conn.setDoOutput(true);
    }
    conn = configureConnection(conn);
    return conn;
  }

  private <T> T call(HttpURLConnection conn, Object jsonOutput,
      int expectedResponse, Class<T> klass) throws IOException {
    return call(conn, jsonOutput, expectedResponse, klass, authRetry);
  }

  private <T> T call(HttpURLConnection conn, Object jsonOutput,
      int expectedResponse, Class<T> klass, int authRetryCount)
      throws IOException {
    T ret = null;
    OutputStream os = null;
    try {
      if (jsonOutput != null) {
        os = conn.getOutputStream();
        writeJson(jsonOutput, os);
      }
    } catch (IOException ex) {
      // The payload is not serialized if getOutputStream fails.
      // Calling getInputStream will issue the put/post request with no payload
      // which causes HTTP 500 server error.
      if (os == null) {
        conn.disconnect();
      } else {
        IOUtils.closeStream(conn.getInputStream());
      }
      throw ex;
    }
    if ((conn.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN
        && (conn.getResponseMessage().equals(ANONYMOUS_REQUESTS_DISALLOWED) ||
            conn.getResponseMessage().contains(INVALID_SIGNATURE)))
        || conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      // Ideally, this should happen only when there is an Authentication
      // failure. Unfortunately, the AuthenticationFilter returns 403 when it
      // cannot authenticate (Since a 401 requires Server to send
      // WWW-Authenticate header as well)..
      if (LOG.isDebugEnabled()) {
        LOG.debug("Response={}({}), resetting authToken",
            conn.getResponseCode(), conn.getResponseMessage());
      }
      KMSClientProvider.this.authToken =
          new DelegationTokenAuthenticatedURL.Token();
      if (authRetryCount > 0) {
        String contentType = conn.getRequestProperty(CONTENT_TYPE);
        String requestMethod = conn.getRequestMethod();
        URL url = conn.getURL();
        conn = createConnection(url, requestMethod);
        conn.setRequestProperty(CONTENT_TYPE, contentType);
        return call(conn, jsonOutput, expectedResponse, klass,
            authRetryCount - 1);
      }
    }
    HttpExceptionUtils.validateResponse(conn, expectedResponse);
    if (conn.getContentType() != null
        && conn.getContentType().trim().toLowerCase()
            .startsWith(APPLICATION_JSON_MIME)
        && klass != null) {
      ObjectMapper mapper = new ObjectMapper();
      InputStream is = null;
      try {
        is = conn.getInputStream();
        ret = mapper.readValue(is, klass);
      } finally {
        IOUtils.closeStream(is);
      }
    }
    return ret;
  }

  public static class KMSKeyVersion extends KeyVersion {
    public KMSKeyVersion(String keyName, String versionName, byte[] material) {
      super(keyName, versionName, material);
    }
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    checkNotEmpty(versionName, "versionName");
    URL url = createURL(KMSRESTConstants.KEY_VERSION_RESOURCE,
        versionName, null, null);
    HttpURLConnection conn = createConnection(url, HTTP_GET);
    Map response = call(conn, null, HttpURLConnection.HTTP_OK, Map.class);
    return parseJSONKeyVersion(response);
  }

  @Override
  public KeyVersion getCurrentKey(String name) throws IOException {
    checkNotEmpty(name, "name");
    URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name,
        KMSRESTConstants.CURRENT_VERSION_SUB_RESOURCE, null);
    HttpURLConnection conn = createConnection(url, HTTP_GET);
    Map response = call(conn, null, HttpURLConnection.HTTP_OK, Map.class);
    return parseJSONKeyVersion(response);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getKeys() throws IOException {
    URL url = createURL(KMSRESTConstants.KEYS_NAMES_RESOURCE, null, null,
        null);
    HttpURLConnection conn = createConnection(url, HTTP_GET);
    List response = call(conn, null, HttpURLConnection.HTTP_OK, List.class);
    return (List<String>) response;
  }

  public static class KMSMetadata extends Metadata {
    public KMSMetadata(String cipher, int bitLength, String description,
        Map<String, String> attributes, Date created, int versions) {
      super(cipher, bitLength, description, attributes, created, versions);
    }
  }

  // breaking keyNames into sets to keep resulting URL undler 2000 chars
  private List<String[]> createKeySets(String[] keyNames) {
    List<String[]> list = new ArrayList<String[]>();
    List<String> batch = new ArrayList<String>();
    int batchLen = 0;
    for (String name : keyNames) {
      int additionalLen = KMSRESTConstants.KEY.length() + 1 + name.length();
      batchLen += additionalLen;
      // topping at 1500 to account for initial URL and encoded names
      if (batchLen > 1500) {
        list.add(batch.toArray(new String[batch.size()]));
        batch = new ArrayList<String>();
        batchLen = additionalLen;
      }
      batch.add(name);
    }
    if (!batch.isEmpty()) {
      list.add(batch.toArray(new String[batch.size()]));
    }
    return list;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Metadata[] getKeysMetadata(String ... keyNames) throws IOException {
    List<Metadata> keysMetadata = new ArrayList<Metadata>();
    List<String[]> keySets = createKeySets(keyNames);
    for (String[] keySet : keySets) {
      if (keyNames.length > 0) {
        Map<String, Object> queryStr = new HashMap<String, Object>();
        queryStr.put(KMSRESTConstants.KEY, keySet);
        URL url = createURL(KMSRESTConstants.KEYS_METADATA_RESOURCE, null,
            null, queryStr);
        HttpURLConnection conn = createConnection(url, HTTP_GET);
        List<Map> list = call(conn, null, HttpURLConnection.HTTP_OK, List.class);
        for (Map map : list) {
          keysMetadata.add(parseJSONMetadata(map));
        }
      }
    }
    return keysMetadata.toArray(new Metadata[keysMetadata.size()]);
  }

  private KeyVersion createKeyInternal(String name, byte[] material,
      Options options)
      throws NoSuchAlgorithmException, IOException {
    checkNotEmpty(name, "name");
    checkNotNull(options, "options");
    Map<String, Object> jsonKey = new HashMap<String, Object>();
    jsonKey.put(KMSRESTConstants.NAME_FIELD, name);
    jsonKey.put(KMSRESTConstants.CIPHER_FIELD, options.getCipher());
    jsonKey.put(KMSRESTConstants.LENGTH_FIELD, options.getBitLength());
    if (material != null) {
      jsonKey.put(KMSRESTConstants.MATERIAL_FIELD,
          Base64.encodeBase64String(material));
    }
    if (options.getDescription() != null) {
      jsonKey.put(KMSRESTConstants.DESCRIPTION_FIELD,
          options.getDescription());
    }
    if (options.getAttributes() != null && !options.getAttributes().isEmpty()) {
      jsonKey.put(KMSRESTConstants.ATTRIBUTES_FIELD, options.getAttributes());
    }
    URL url = createURL(KMSRESTConstants.KEYS_RESOURCE, null, null, null);
    HttpURLConnection conn = createConnection(url, HTTP_POST);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
    Map response = call(conn, jsonKey, HttpURLConnection.HTTP_CREATED,
        Map.class);
    return parseJSONKeyVersion(response);
  }

  @Override
  public KeyVersion createKey(String name, Options options)
      throws NoSuchAlgorithmException, IOException {
    return createKeyInternal(name, null, options);
  }

  @Override
  public KeyVersion createKey(String name, byte[] material, Options options)
      throws IOException {
    checkNotNull(material, "material");
    try {
      return createKeyInternal(name, material, options);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("It should not happen", ex);
    }
  }

  @Override
  public void invalidateCache(String name) throws IOException {
    checkNotEmpty(name, "name");
    final URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name,
        KMSRESTConstants.INVALIDATECACHE_RESOURCE, null);
    final HttpURLConnection conn = createConnection(url, HTTP_POST);
    // invalidate the server cache first, then drain local cache.
    call(conn, null, HttpURLConnection.HTTP_OK, null);
    drain(name);
  }

  private KeyVersion rollNewVersionInternal(String name, byte[] material)
      throws NoSuchAlgorithmException, IOException {
    checkNotEmpty(name, "name");
    Map<String, String> jsonMaterial = new HashMap<String, String>();
    if (material != null) {
      jsonMaterial.put(KMSRESTConstants.MATERIAL_FIELD,
          Base64.encodeBase64String(material));
    }
    URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name, null, null);
    HttpURLConnection conn = createConnection(url, HTTP_POST);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
    Map response = call(conn, jsonMaterial,
        HttpURLConnection.HTTP_OK, Map.class);
    KeyVersion keyVersion = parseJSONKeyVersion(response);
    invalidateCache(name);
    return keyVersion;
  }


  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    return rollNewVersionInternal(name, null);
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    checkNotNull(material, "material");
    try {
      return rollNewVersionInternal(name, material);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("It should not happen", ex);
    }
  }

  @Override
  public EncryptedKeyVersion generateEncryptedKey(
      String encryptionKeyName) throws IOException, GeneralSecurityException {
    try {
      return encKeyVersionQueue.getNext(encryptionKeyName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SocketTimeoutException) {
        throw (SocketTimeoutException)e.getCause();
      }
      throw new IOException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public KeyVersion decryptEncryptedKey(
      EncryptedKeyVersion encryptedKeyVersion) throws IOException,
                                                      GeneralSecurityException {
    checkNotNull(encryptedKeyVersion.getEncryptionKeyVersionName(),
        "versionName");
    checkNotNull(encryptedKeyVersion.getEncryptedKeyIv(), "iv");
    Preconditions.checkArgument(
        encryptedKeyVersion.getEncryptedKeyVersion().getVersionName()
            .equals(KeyProviderCryptoExtension.EEK),
        "encryptedKey version name must be '%s', is '%s'",
        KeyProviderCryptoExtension.EEK,
        encryptedKeyVersion.getEncryptedKeyVersion().getVersionName()
    );
    checkNotNull(encryptedKeyVersion.getEncryptedKeyVersion(), "encryptedKey");
    Map<String, String> params = new HashMap<String, String>();
    params.put(KMSRESTConstants.EEK_OP, KMSRESTConstants.EEK_DECRYPT);
    Map<String, Object> jsonPayload = new HashMap<String, Object>();
    jsonPayload.put(KMSRESTConstants.NAME_FIELD,
        encryptedKeyVersion.getEncryptionKeyName());
    jsonPayload.put(KMSRESTConstants.IV_FIELD, Base64.encodeBase64String(
        encryptedKeyVersion.getEncryptedKeyIv()));
    jsonPayload.put(KMSRESTConstants.MATERIAL_FIELD, Base64.encodeBase64String(
            encryptedKeyVersion.getEncryptedKeyVersion().getMaterial()));
    URL url = createURL(KMSRESTConstants.KEY_VERSION_RESOURCE,
        encryptedKeyVersion.getEncryptionKeyVersionName(),
        KMSRESTConstants.EEK_SUB_RESOURCE, params);
    HttpURLConnection conn = createConnection(url, HTTP_POST);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
    Map response =
        call(conn, jsonPayload, HttpURLConnection.HTTP_OK, Map.class);
    return parseJSONKeyVersion(response);
  }

  @Override
  public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
      throws IOException, GeneralSecurityException {
    checkNotNull(ekv.getEncryptionKeyVersionName(), "versionName");
    checkNotNull(ekv.getEncryptedKeyIv(), "iv");
    checkNotNull(ekv.getEncryptedKeyVersion(), "encryptedKey");
    Preconditions.checkArgument(ekv.getEncryptedKeyVersion().getVersionName()
            .equals(KeyProviderCryptoExtension.EEK),
        "encryptedKey version name must be '%s', is '%s'",
        KeyProviderCryptoExtension.EEK,
        ekv.getEncryptedKeyVersion().getVersionName());
    final Map<String, String> params = new HashMap<>();
    params.put(KMSRESTConstants.EEK_OP, KMSRESTConstants.EEK_REENCRYPT);
    final Map<String, Object> jsonPayload = new HashMap<>();
    jsonPayload.put(KMSRESTConstants.NAME_FIELD, ekv.getEncryptionKeyName());
    jsonPayload.put(KMSRESTConstants.IV_FIELD,
        Base64.encodeBase64String(ekv.getEncryptedKeyIv()));
    jsonPayload.put(KMSRESTConstants.MATERIAL_FIELD,
        Base64.encodeBase64String(ekv.getEncryptedKeyVersion().getMaterial()));
    final URL url = createURL(KMSRESTConstants.KEY_VERSION_RESOURCE,
        ekv.getEncryptionKeyVersionName(), KMSRESTConstants.EEK_SUB_RESOURCE,
        params);
    final HttpURLConnection conn = createConnection(url, HTTP_POST);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
    final Map response =
        call(conn, jsonPayload, HttpURLConnection.HTTP_OK, Map.class);
    return parseJSONEncKeyVersion(ekv.getEncryptionKeyName(), response);
  }

  @Override
  public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
      throws IOException, GeneralSecurityException {
    checkNotNull(ekvs, "ekvs");
    if (ekvs.isEmpty()) {
      return;
    }
    final List<Map> jsonPayload = new ArrayList<>();
    String keyName = null;
    for (EncryptedKeyVersion ekv : ekvs) {
      checkNotNull(ekv.getEncryptionKeyName(), "keyName");
      checkNotNull(ekv.getEncryptionKeyVersionName(), "versionName");
      checkNotNull(ekv.getEncryptedKeyIv(), "iv");
      checkNotNull(ekv.getEncryptedKeyVersion(), "encryptedKey");
      Preconditions.checkArgument(ekv.getEncryptedKeyVersion().getVersionName()
              .equals(KeyProviderCryptoExtension.EEK),
          "encryptedKey version name must be '%s', is '%s'",
          KeyProviderCryptoExtension.EEK,
          ekv.getEncryptedKeyVersion().getVersionName());
      if (keyName == null) {
        keyName = ekv.getEncryptionKeyName();
      } else {
        Preconditions.checkArgument(keyName.equals(ekv.getEncryptionKeyName()),
            "All EncryptedKey must have the same key name.");
      }
      jsonPayload.add(KMSUtil.toJSON(ekv));
    }
    final URL url = createURL(KMSRESTConstants.KEY_RESOURCE, keyName,
        KMSRESTConstants.REENCRYPT_BATCH_SUB_RESOURCE, null);
    final HttpURLConnection conn = createConnection(url, HTTP_POST);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
    final List<Map> response =
        call(conn, jsonPayload, HttpURLConnection.HTTP_OK, List.class);
    Preconditions.checkArgument(response.size() == ekvs.size(),
        "Response size is different than input size.");
    for (int i = 0; i < response.size(); ++i) {
      final Map item = response.get(i);
      final EncryptedKeyVersion ekv = parseJSONEncKeyVersion(keyName, item);
      ekvs.set(i, ekv);
    }
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    checkNotEmpty(name, "name");
    URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name,
        KMSRESTConstants.VERSIONS_SUB_RESOURCE, null);
    HttpURLConnection conn = createConnection(url, HTTP_GET);
    List response = call(conn, null, HttpURLConnection.HTTP_OK, List.class);
    List<KeyVersion> versions = null;
    if (!response.isEmpty()) {
      versions = new ArrayList<KeyVersion>();
      for (Object obj : response) {
        versions.add(parseJSONKeyVersion((Map) obj));
      }
    }
    return versions;
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    checkNotEmpty(name, "name");
    URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name,
        KMSRESTConstants.METADATA_SUB_RESOURCE, null);
    HttpURLConnection conn = createConnection(url, HTTP_GET);
    Map response = call(conn, null, HttpURLConnection.HTTP_OK, Map.class);
    return parseJSONMetadata(response);
  }

  @Override
  public void deleteKey(String name) throws IOException {
    checkNotEmpty(name, "name");
    URL url = createURL(KMSRESTConstants.KEY_RESOURCE, name, null, null);
    HttpURLConnection conn = createConnection(url, HTTP_DELETE);
    call(conn, null, HttpURLConnection.HTTP_OK, null);
  }

  @Override
  public void flush() throws IOException {
    // NOP
    // the client does not keep any local state, thus flushing is not required
    // because of the client.
    // the server should not keep in memory state on behalf of clients either.
  }

  @Override
  public void warmUpEncryptedKeys(String... keyNames)
      throws IOException {
    try {
      encKeyVersionQueue.initializeQueuesForKeys(keyNames);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void drain(String keyName) {
    encKeyVersionQueue.drain(keyName);
  }

  @VisibleForTesting
  public int getEncKeyQueueSize(String keyName) {
    return encKeyVersionQueue.getSize(keyName);
  }

  @Override
  public long renewDelegationToken(final Token<?> dToken) throws IOException {
    try {
      final String doAsUser = getDoAsUser();
      final DelegationTokenAuthenticatedURL.Token token =
          generateDelegationToken(dToken);
      final URL url = createURL(null, null, null, null);
      LOG.debug("Renewing delegation token {} with url:{}, as:{}",
          token, url, doAsUser);
      final DelegationTokenAuthenticatedURL authUrl =
          new DelegationTokenAuthenticatedURL(configurator);
      return getActualUgi().doAs(
          new PrivilegedExceptionAction<Long>() {
            @Override
            public Long run() throws Exception {
              return authUrl.renewDelegationToken(url, token, doAsUser);
            }
          }
      );
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException) ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  @Override
  public Void cancelDelegationToken(final Token<?> dToken) throws IOException {
    try {
      final String doAsUser = getDoAsUser();
      final DelegationTokenAuthenticatedURL.Token token =
          generateDelegationToken(dToken);
      return getActualUgi().doAs(
          new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              final URL url = createURL(null, null, null, null);
              LOG.debug("Cancelling delegation token {} with url:{}, as:{}",
                  dToken, url, doAsUser);
              final DelegationTokenAuthenticatedURL authUrl =
                  new DelegationTokenAuthenticatedURL(configurator);
              authUrl.cancelDelegationToken(url, token, doAsUser);
              return null;
            }
          }
      );
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException) ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  /**
   * Get the doAs user name.
   *
   * 'actualUGI' is the UGI of the user creating the client
   * It is possible that the creator of the KMSClientProvier
   * calls this method on behalf of a proxyUser (the doAsUser).
   * In which case this call has to be made as the proxy user.
   *
   * @return the doAs user name.
   * @throws IOException
   */
  private String getDoAsUser() throws IOException {
    UserGroupInformation currentUgi = UserGroupInformation.getCurrentUser();
    return (currentUgi.getAuthenticationMethod() ==
        UserGroupInformation.AuthenticationMethod.PROXY)
        ? currentUgi.getShortUserName() : null;
  }

  /**
   * Generate a DelegationTokenAuthenticatedURL.Token from the given generic
   * typed delegation token.
   *
   * @param dToken The delegation token.
   * @return The DelegationTokenAuthenticatedURL.Token, with its delegation
   *         token set to the delegation token passed in.
   */
  private DelegationTokenAuthenticatedURL.Token generateDelegationToken(
      final Token<?> dToken) {
    DelegationTokenAuthenticatedURL.Token token =
        new DelegationTokenAuthenticatedURL.Token();
    Token<AbstractDelegationTokenIdentifier> dt =
        new Token<>(dToken.getIdentifier(), dToken.getPassword(),
            dToken.getKind(), dToken.getService());
    token.setDelegationToken(dt);
    return token;
  }

  @Override
  public Token<?>[] addDelegationTokens(final String renewer,
      Credentials credentials) throws IOException {
    Token<?>[] tokens = null;
    Token<?> token = credentials.getToken(dtService);
    if (token == null) {
      final URL url = createURL(null, null, null, null);
      final DelegationTokenAuthenticatedURL authUrl =
          new DelegationTokenAuthenticatedURL(configurator);
      try {
        final String doAsUser = getDoAsUser();
        token = getActualUgi().doAs(new PrivilegedExceptionAction<Token<?>>() {
          @Override
          public Token<?> run() throws Exception {
            // Not using the cached token here.. Creating a new token here
            // everytime.
            LOG.debug("Getting new token from {}, renewer:{}", url, renewer);
            return authUrl.getDelegationToken(url,
                new DelegationTokenAuthenticatedURL.Token(), renewer, doAsUser);
          }
        });
        if (token != null) {
          LOG.debug("New token received: ({})", token);
          credentials.addToken(token.getService(), token);
          tokens = new Token<?>[] { token };
        } else {
          throw new IOException("Got NULL as delegation token");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new IOException(e);
        }
      }
    }
    return tokens;
  }

  private boolean containsKmsDt(UserGroupInformation ugi) throws IOException {
    // Add existing credentials from the UGI, since provider is cached.
    Credentials creds = ugi.getCredentials();
    if (!creds.getAllTokens().isEmpty()) {
      LOG.debug("Searching for token that matches service: {}", dtService);
      org.apache.hadoop.security.token.Token<? extends TokenIdentifier>
          dToken = creds.getToken(dtService);
      if (dToken != null) {
        return true;
      }
    }
    return false;
  }

  private UserGroupInformation getActualUgi() throws IOException {
    final UserGroupInformation currentUgi = UserGroupInformation
        .getCurrentUser();

    UserGroupInformation.logAllUserInfo(LOG, currentUgi);

    // Use current user by default
    UserGroupInformation actualUgi = currentUgi;
    if (currentUgi.getRealUser() != null) {
      // Use real user for proxy user
      actualUgi = currentUgi.getRealUser();
    }
    if (UserGroupInformation.isSecurityEnabled() &&
        !containsKmsDt(actualUgi) &&
        !actualUgi.hasKerberosCredentials()) {
      // Use login user is only necessary when Kerberos is enabled
      // but the actual user does not have either
      // Kerberos credential or KMS delegation token for KMS operations
      LOG.debug("Using loginUser when Kerberos is enabled but the actual user" +
          " does not have either KMS Delegation Token or Kerberos Credentials");
      actualUgi = UserGroupInformation.getLoginUser();
    }
    return actualUgi;
  }

  /**
   * Shutdown valueQueue executor threads
   */
  @Override
  public void close() throws IOException {
    try {
      encKeyVersionQueue.shutdown();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (sslFactory != null) {
        sslFactory.destroy();
        sslFactory = null;
      }
    }
  }

  @VisibleForTesting
  String getKMSUrl() {
    return kmsUrl.toString();
  }
}
