/*
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
package org.apache.hadoop.ozone.s3;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;
import org.apache.hadoop.ozone.s3.header.Credential;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_TOKEN_CREATION_ERROR;

/**
 * Parser to process AWS v4 auth request. Creates string to sign and auth
 * header. For more details refer to AWS documentation https://docs.aws
 * .amazon.com/general/latest/gr/sigv4-create-canonical-request.html.
 **/
public class AWSV4AuthParser implements AWSAuthParser {

  private final static Logger LOG =
      LoggerFactory.getLogger(AWSV4AuthParser.class);
  private MultivaluedMap<String, String> headerMap;
  private MultivaluedMap<String, String> queryMap;
  private String uri;
  private String method;
  private AuthorizationHeaderV4 v4Header;
  private String stringToSign;
  private String amzContentPayload;

  public AWSV4AuthParser(ContainerRequestContext context)
      throws OS3Exception {
    this.headerMap = context.getHeaders();
    this.queryMap = context.getUriInfo().getQueryParameters();
    try {
      this.uri = new URI(context.getUriInfo().getRequestUri()
          .getPath().replaceAll("\\/+",
              "/")).normalize().getPath();
    } catch (URISyntaxException e) {
      throw S3_TOKEN_CREATION_ERROR;
    }

    this.method = context.getMethod();
    v4Header = new AuthorizationHeaderV4(
        headerMap.getFirst(AUTHORIZATION_HEADER));
  }

  public void parse() throws Exception {
    StringBuilder strToSign = new StringBuilder();

    // According to AWS sigv4 documentation, authorization header should be
    // in following format.
    // Authorization: algorithm Credential=access key ID/credential scope,
    // SignedHeaders=SignedHeaders, Signature=signature

    // Construct String to sign in below format.
    // StringToSign =
    //    Algorithm + \n +
    //    RequestDateTime + \n +
    //    CredentialScope + \n +
    //    HashedCanonicalRequest
    String algorithm, requestDateTime, credentialScope, canonicalRequest;
    algorithm = v4Header.getAlgorithm();
    requestDateTime = headerMap.getFirst(X_AMAZ_DATE);
    Credential credential = v4Header.getCredentialObj();
    credentialScope = String.format("%s/%s/%s/%s", credential.getDate(),
        credential.getAwsRegion(), credential.getAwsService(),
        credential.getAwsRequest());

    // If the absolute path is empty, use a forward slash (/)
    uri = (uri.trim().length() > 0) ? uri : "/";
    // Encode URI and preserve forward slashes
    strToSign.append(algorithm + NEWLINE);
    strToSign.append(requestDateTime + NEWLINE);
    strToSign.append(credentialScope + NEWLINE);

    canonicalRequest = buildCanonicalRequest();
    strToSign.append(hash(canonicalRequest));
    LOG.debug("canonicalRequest:[{}]", canonicalRequest);

    headerMap.keySet().forEach(k -> LOG.trace("Header:{},value:{}", k,
        headerMap.get(k)));

    LOG.debug("StringToSign:[{}]", strToSign);
    stringToSign = strToSign.toString();
  }

  private String buildCanonicalRequest() throws OS3Exception {
    Iterable<String> parts = split("/", uri);
    List<String> encParts = new ArrayList<>();
    for (String p : parts) {
      encParts.add(urlEncode(p));
    }
    String canonicalUri = join("/", encParts);

    String canonicalQueryStr = getQueryParamString();

    StringBuilder canonicalHeaders = new StringBuilder();

    for (String header : v4Header.getSignedHeaders()) {
      List<String> headerValue = new ArrayList<>();
      canonicalHeaders.append(header.toLowerCase());
      canonicalHeaders.append(":");
      for (String originalHeader : headerMap.keySet()) {
        if (originalHeader.toLowerCase().equals(header)) {
          headerValue.add(headerMap.getFirst(originalHeader).trim());
        }
      }

      if (headerValue.size() == 0) {
        throw new RuntimeException("Header " + header + " not present in " +
            "request");
      }
      if (headerValue.size() > 1) {
        Collections.sort(headerValue);
      }

      // Set for testing purpose only to skip date and host validation.
      validateSignedHeader(header, headerValue.get(0));

      canonicalHeaders.append(join(",", headerValue));
      canonicalHeaders.append(NEWLINE);
    }

    String payloadHash;
    if (UNSIGNED_PAYLOAD.equals(
        headerMap.get(X_AMZ_CONTENT_SHA256))) {
      payloadHash = UNSIGNED_PAYLOAD;
    } else {
      payloadHash = headerMap.getFirst(X_AMZ_CONTENT_SHA256);
    }

    String signedHeaderStr = v4Header.getSignedHeaderString();
    String canonicalRequest = method + NEWLINE
        + canonicalUri + NEWLINE
        + canonicalQueryStr + NEWLINE
        + canonicalHeaders + NEWLINE
        + signedHeaderStr + NEWLINE
        + payloadHash;

    return canonicalRequest;
  }

  @VisibleForTesting
  void validateSignedHeader(String header, String headerValue)
      throws OS3Exception {
    switch (header) {
    case HOST:
      try {
        URI hostUri = new URI(headerValue);
        InetAddress.getByName(hostUri.getHost());
        // TODO: Validate if current request is coming from same host.
      } catch (UnknownHostException|URISyntaxException e) {
        LOG.error("Host value mentioned in signed header is not valid. " +
            "Host:{}", headerValue);
        throw S3_TOKEN_CREATION_ERROR;
      }
      break;
    case X_AMAZ_DATE:
      LocalDate date = LocalDate.parse(headerValue, TIME_FORMATTER);
      LocalDate now = LocalDate.now();
      if (date.isBefore(now.minus(PRESIGN_URL_MAX_EXPIRATION_SECONDS, SECONDS))
          || date.isAfter(now.plus(PRESIGN_URL_MAX_EXPIRATION_SECONDS,
          SECONDS))) {
        LOG.error("AWS date not in valid range. Request timestamp:{} should " +
                "not be older than {} seconds.", headerValue,
            PRESIGN_URL_MAX_EXPIRATION_SECONDS);
        throw S3_TOKEN_CREATION_ERROR;
      }
      break;
    case X_AMZ_CONTENT_SHA256:
      // TODO: Construct request payload and match HEX(SHA256(requestPayload))
      break;
    default:
      break;
    }
  }

  /**
   * String join that also works with empty strings.
   *
   * @return joined string
   */
  private static String join(String glue, List<String> parts) {
    StringBuilder result = new StringBuilder();
    boolean addSeparator = false;
    for (String p : parts) {
      if (addSeparator) {
        result.append(glue);
      }
      result.append(p);
      addSeparator = true;
    }
    return result.toString();
  }

  /**
   * Returns matching strings.
   *
   * @param regex Regular expression to split by
   * @param whole The string to split
   * @return pieces
   */
  private static Iterable<String> split(String regex, String whole) {
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(whole);
    List<String> result = new ArrayList<>();
    int pos = 0;
    while (m.find()) {
      result.add(whole.substring(pos, m.start()));
      pos = m.end();
    }
    result.add(whole.substring(pos));
    return result;
  }

  private String urlEncode(String str) {
    try {

      return URLEncoder.encode(str, UTF_8.name())
          .replaceAll("\\+", "%20")
          .replaceAll("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private String getQueryParamString() {
    List<String> params = new ArrayList<>(queryMap.keySet());

    // Sort by name, then by value
    Collections.sort(params, (o1, o2) -> o1.equals(o2) ?
        queryMap.getFirst(o1).compareTo(queryMap.getFirst(o2)) :
        o1.compareTo(o2));

    StringBuilder result = new StringBuilder();
    for (String p : params) {
      if (result.length() > 0) {
        result.append("&");
      }
      result.append(urlEncode(p));
      result.append('=');

      result.append(urlEncode(queryMap.getFirst(p)));
    }
    return result.toString();
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(UTF_8));
    return Hex.encode(md.digest()).toLowerCase();
  }

  public String getAwsAccessId() {
    return v4Header.getAccessKeyID();
  }

  public String getSignature() {
    return v4Header.getSignature();
  }

  public String getStringToSign() throws Exception {
    return stringToSign;
  }
}
