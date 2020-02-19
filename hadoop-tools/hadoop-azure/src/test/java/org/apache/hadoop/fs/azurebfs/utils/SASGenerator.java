package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.services.AbfsUriQueryBuilder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;


/**
 * Created by tmarq on 2/17/20.
 */
public class SASGenerator {

  private static final String HMAC_SHA256 = "HmacSHA256";
  private Mac hmacSha256;
  private byte[] key;

  public SASGenerator(byte[] key) {
    this.key = key;
    initializeMac();
  }

  public String getContainerSASWithFullControl(String accountName, String containerName) {
    String sp = "rwdl";
    String se = "2021-01-01";
    String sv = "2018-11-09";
    String sr = "c";

    // compute string to sign
    StringBuilder sb = new StringBuilder();
    // sp
    sb.append(sp);
    sb.append("\n");
    // st
    sb.append("\n");
    // se
    sb.append(se);
    sb.append("\n");
    // canonicalized resource
    sb.append("/blob/");
    sb.append(accountName);
    sb.append("/");
    sb.append(containerName);
    sb.append("\n");
    // si
    sb.append("\n");
    // sip
    sb.append("\n");
    // spr
    sb.append("\n");
    // sv
    sb.append(sv);
    sb.append("\n");
    // sr
    sb.append(sr);
    sb.append("\n");
    // For ResponseCacheControl
    sb.append("\n");
    // For ResponseContentDisposition
    sb.append("\n");
    // For ResponseContentEncoding
    sb.append("\n");
    // For ResponseContentLanguage
    sb.append("\n");
    // For ResponseContentType
    sb.append("\n");

    String stringToSign = sb.toString();
    String sig = computeHmac256(stringToSign);

    AbfsUriQueryBuilder qb = new AbfsUriQueryBuilder();
    qb.addQuery("sp", sp);
    qb.addQuery("se", se);
    qb.addQuery("sv", sv);
    qb.addQuery("sr", sr);
    qb.addQuery("sig", sig);
    return qb.toString();
  }

  private void initializeMac() {
    // Initializes the HMAC-SHA256 Mac and SecretKey.
    try {
      hmacSha256 = Mac.getInstance(HMAC_SHA256);
      hmacSha256.init(new SecretKeySpec(key, HMAC_SHA256));
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String computeHmac256(final String stringToSign) {
    byte[] utf8Bytes;
    try {
      utf8Bytes = stringToSign.getBytes(AbfsHttpConstants.UTF_8);
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
    byte[] hmac;
    synchronized (this) {
      hmac = hmacSha256.doFinal(utf8Bytes);
    }
    return Base64.encode(hmac);
  }
}
