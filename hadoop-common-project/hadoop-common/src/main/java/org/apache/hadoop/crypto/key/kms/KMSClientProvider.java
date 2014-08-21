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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.http.client.utils.URIBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KMS client <code>KeyProvider</code> implementation.
 */
@InterfaceAudience.Private
public class KMSClientProvider extends KeyProvider {

  public static final String SCHEME_NAME = "kms";

  private static final String UTF8 = "UTF-8";

  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON_MIME = "application/json";

  private static final String HTTP_GET = "GET";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_PUT = "PUT";
  private static final String HTTP_DELETE = "DELETE";

  private static KeyVersion parseJSONKeyVersion(Map valueMap) {
    KeyVersion keyVersion = null;
    if (!valueMap.isEmpty()) {
      byte[] material = (valueMap.containsKey(KMSRESTConstants.MATERIAL_FIELD))
          ? Base64.decodeBase64((String) valueMap.get(KMSRESTConstants.MATERIAL_FIELD))
          : null;
      keyVersion = new KMSKeyVersion((String)
          valueMap.get(KMSRESTConstants.VERSION_NAME_FIELD), material);
    }
    return keyVersion;
  }

  private static Metadata parseJSONMetadata(Map valueMap) {
    Metadata metadata = null;
    if (!valueMap.isEmpty()) {
      metadata = new KMSMetadata(
          (String) valueMap.get(KMSRESTConstants.CIPHER_FIELD),
          (Integer) valueMap.get(KMSRESTConstants.LENGTH_FIELD),
          (String) valueMap.get(KMSRESTConstants.DESCRIPTION_FIELD),
          new Date((Long) valueMap.get(KMSRESTConstants.CREATED_FIELD)),
          (Integer) valueMap.get(KMSRESTConstants.VERSIONS_FIELD));
    }
    return metadata;
  }

  private static void writeJson(Map map, OutputStream os) throws IOException {
    Writer writer = new OutputStreamWriter(os);
    ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.writerWithDefaultPrettyPrinter().writeValue(writer, map);
  }

  /**
   * The factory to create KMSClientProvider, which is used by the
   * ServiceLoader.
   */
  public static class Factory extends KeyProviderFactory {

    @Override
    public KeyProvider createProvider(URI providerName, Configuration conf)
        throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new KMSClientProvider(providerName, conf);
      }
      return null;
    }
  }

  public static <T> T checkNotNull(T o, String name)
      throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Parameter '" + name +
          "' cannot be null");
    }
    return o;
  }

  public static String checkNotEmpty(String s, String name)
      throws IllegalArgumentException {
    checkNotNull(s, name);
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Parameter '" + name +
          "' cannot be empty");
    }
    return s;
  }

  private String kmsUrl;
  private SSLFactory sslFactory;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("KMSClientProvider[");
    sb.append(kmsUrl).append("]");
    return sb.toString();
  }

  public KMSClientProvider(URI uri, Configuration conf) throws IOException {
    Path path = unnestUri(uri);
    URL url = path.toUri().toURL();
    kmsUrl = createServiceURL(url);
    if ("https".equalsIgnoreCase(url.getProtocol())) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
    }
  }

  private String createServiceURL(URL url) throws IOException {
    String str = url.toExternalForm();
    if (str.endsWith("/")) {
      str = str.substring(0, str.length() - 1);
    }
    return new URL(str + KMSRESTConstants.SERVICE_VERSION + "/").
        toExternalForm();
  }

  private URL createURL(String collection, String resource, String subResource,
      Map<String, ?> parameters) throws IOException {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(kmsUrl);
      sb.append(collection);
      if (resource != null) {
        sb.append("/").append(URLEncoder.encode(resource, UTF8));
      }
      if (subResource != null) {
        sb.append("/").append(subResource);
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

  private HttpURLConnection createConnection(URL url, String method)
      throws IOException {
    HttpURLConnection conn;
    try {
      AuthenticatedURL authUrl = new AuthenticatedURL(new PseudoAuthenticator(),
          sslFactory);
      conn = authUrl.openConnection(url, new AuthenticatedURL.Token());
    } catch (AuthenticationException ex) {
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

  // trick, riding on generics to throw an undeclared exception

  private static void throwEx(Throwable ex) {
    KMSClientProvider.<RuntimeException>throwException(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwException(Throwable ex)
      throws E {
    throw (E) ex;
  }

  @SuppressWarnings("unchecked")
  private static void validateResponse(HttpURLConnection conn, int expected)
      throws IOException {
    int status = conn.getResponseCode();
    if (status != expected) {
      InputStream es = null;
      try {
        es = conn.getErrorStream();
        ObjectMapper mapper = new ObjectMapper();
        Map json = mapper.readValue(es, Map.class);
        String exClass = (String) json.get(
            KMSRESTConstants.ERROR_EXCEPTION_JSON);
        String exMsg = (String)
            json.get(KMSRESTConstants.ERROR_MESSAGE_JSON);
        Exception toThrow;
        try {
          ClassLoader cl = KMSClientProvider.class.getClassLoader();
          Class klass = cl.loadClass(exClass);
          Constructor constr = klass.getConstructor(String.class);
          toThrow = (Exception) constr.newInstance(exMsg);
        } catch (Exception ex) {
          toThrow = new IOException(MessageFormat.format(
              "HTTP status [{0}], {1}", status, conn.getResponseMessage()));
        }
        throwEx(toThrow);
      } finally {
        if (es != null) {
          es.close();
        }
      }
    }
  }

  private static <T> T call(HttpURLConnection conn, Map jsonOutput,
      int expectedResponse, Class<T> klass)
      throws IOException {
    T ret = null;
    try {
      if (jsonOutput != null) {
        writeJson(jsonOutput, conn.getOutputStream());
      }
    } catch (IOException ex) {
      conn.getInputStream().close();
      throw ex;
    }
    validateResponse(conn, expectedResponse);
    if (APPLICATION_JSON_MIME.equalsIgnoreCase(conn.getContentType())
        && klass != null) {
      ObjectMapper mapper = new ObjectMapper();
      InputStream is = null;
      try {
        is = conn.getInputStream();
        ret = mapper.readValue(is, klass);
      } catch (IOException ex) {
        if (is != null) {
          is.close();
        }
        throw ex;
      } finally {
        if (is != null) {
          is.close();
        }
      }
    }
    return ret;
  }

  public static class KMSKeyVersion extends KeyVersion {
    public KMSKeyVersion(String versionName, byte[] material) {
      super(versionName, material);
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
        Date created, int versions) {
      super(cipher, bitLength, description, created, versions);
    }
  }

  // breaking keyNames into sets to keep resulting URL undler 2000 chars
  private List<String[]> createKeySets(String[] keyNames) {
    List<String[]> list = new ArrayList<String[]>();
    List<String> batch = new ArrayList<String>();
    int batchLen = 0;
    for (String name : keyNames) {
      int additionalLen = KMSRESTConstants.KEY_OP.length() + 1 + name.length();
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
        queryStr.put(KMSRESTConstants.KEY_OP, keySet);
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
    return parseJSONKeyVersion(response);
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

  @VisibleForTesting
  public static String buildVersionName(String name, int version) {
    return KeyProvider.buildVersionName(name, version);
  }
}
