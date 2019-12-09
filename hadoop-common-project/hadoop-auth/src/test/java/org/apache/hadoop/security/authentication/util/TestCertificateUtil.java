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
package org.apache.hadoop.security.authentication.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.interfaces.RSAPublicKey;

import javax.servlet.ServletException;

import org.junit.Test;

public class TestCertificateUtil {

  @Test
  public void testInvalidPEMWithHeaderAndFooter() throws Exception {
    String pem = "-----BEGIN CERTIFICATE-----\n"
        + "MIICOjCCAaOgAwIBAgIJANXi/oWxvJNzMA0GCSqGSIb3DQEBBQUAMF8xCzAJBgNVBAYTAlVTMQ0w"
        + "CwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRvb3AxDTALBgNVBAsTBFRl"
        + "c3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNTAxMDIyMTE5MjRaFw0xNjAxMDIyMTE5MjRaMF8x"
        + "CzAJBgNVBAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRv"
        + "b3AxDTALBgNVBAsTBFRlc3QxEjAQBgNVBAMTCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOB"
        + "jQAwgYkCgYEAwpfpLdi7dWTHNzETt+L7618/dWUQFb/C7o1jIxFgbKOVIB6d5YmvUbJck5PYxFkz"
        + "C25fmU5H71WGOI1Kle5TFDmIo+hqh5xqu1YNRZz9i6D94g+2AyYr9BpvH4ZfdHs7r9AU7c3kq68V"
        + "7OPuuaHb25J8isiOyA3RiWuJGQlXTdkCAwEAATANBgkqhkiG9w0BAQUFAAOBgQAdRUyCUqE9sdim"
        + "Fbll9BuZDKV16WXeWGq+kTd7ETe7l0fqXjq5EnrifOai0L/pXwVvS2jrFkKQRlRxRGUNaeEBZ2Wy"
        + "9aTyR+HGHCfvwoCegc9rAVw/DLaRriSO/jnEXzYK6XLVKH+hx5UXrJ7Oyc7JjZUc3g9kCWORThCX"
        + "Mzc1xA==" + "\n-----END CERTIFICATE-----";
    try {
      CertificateUtil.parseRSAPublicKey(pem);
      fail("Should not have thrown ServletException");
    } catch (ServletException se) {
      assertTrue(se.getMessage().contains("PEM header"));
    }
  }

  @Test
  public void testCorruptPEM() throws Exception {
    String pem = "MIICOjCCAaOgAwIBAgIJANXi/oWxvJNzMA0GCSqGSIb3DQEBBQUAMF8xCzAJBgNVBAYTAlVTMQ0w"
        + "CwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRvb3AxDTALBgNVBAsTBFRl"
        + "c3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNTAxMDIyMTE5MjRaFw0xNjAxMDIyMTE5MjRaMF8x"
        + "CzAJBgNVBAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRv"
        + "b3AxDTALBgNVBAsTBFRlc3QxEjAQBgNVBAMTCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOB"
        + "jQAwgYkCgYEAwpfpLdi7dWTHNzETt+L7618/dWUQFb/C7o1jIxFgbKOVIB6d5YmvUbJck5PYxFkz"
        + "C25fmU5H71WGOI1Kle5TFDmIo+hqh5xqu1YNRZz9i6D94g+2AyYr9BpvH4ZfdHs7r9AU7c3kq68V"
        + "7OPuuaHb25J8isiOyA3RiWuJGQlXTdkCAwEAATANBgkqhkiG9w0BAQUFAAOBgQAdRUyCUqE9sdim"
        + "Fbll9BuZDKV16WXeWGq+kTd7ETe7l0fqXjq5EnrifOai0L/pXwVvS2jrFkKQRlRxRGUNaeEBZ2Wy"
        + "9aTyR+HGHCfvwoCegc9rAVw/DLaRriSO/jnEXzYK6XLVKH+hx5UXrJ7Oyc7JjZUc3g9kCWORThCX"
        + "Mzc1xA++";
    try {
      CertificateUtil.parseRSAPublicKey(pem);
      fail("Should not have thrown ServletException");
    } catch (ServletException se) {
      assertTrue(se.getMessage().contains("corrupt"));
    }
  }

  @Test
  public void testValidPEM() throws Exception {
    String pem = "MIICOjCCAaOgAwIBAgIJANXi/oWxvJNzMA0GCSqGSIb3DQEBBQUAMF8xCzAJBgNVBAYTAlVTMQ0w"
        + "CwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRvb3AxDTALBgNVBAsTBFRl"
        + "c3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNTAxMDIyMTE5MjRaFw0xNjAxMDIyMTE5MjRaMF8x"
        + "CzAJBgNVBAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRv"
        + "b3AxDTALBgNVBAsTBFRlc3QxEjAQBgNVBAMTCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOB"
        + "jQAwgYkCgYEAwpfpLdi7dWTHNzETt+L7618/dWUQFb/C7o1jIxFgbKOVIB6d5YmvUbJck5PYxFkz"
        + "C25fmU5H71WGOI1Kle5TFDmIo+hqh5xqu1YNRZz9i6D94g+2AyYr9BpvH4ZfdHs7r9AU7c3kq68V"
        + "7OPuuaHb25J8isiOyA3RiWuJGQlXTdkCAwEAATANBgkqhkiG9w0BAQUFAAOBgQAdRUyCUqE9sdim"
        + "Fbll9BuZDKV16WXeWGq+kTd7ETe7l0fqXjq5EnrifOai0L/pXwVvS2jrFkKQRlRxRGUNaeEBZ2Wy"
        + "9aTyR+HGHCfvwoCegc9rAVw/DLaRriSO/jnEXzYK6XLVKH+hx5UXrJ7Oyc7JjZUc3g9kCWORThCX"
        + "Mzc1xA==";
    try {
      RSAPublicKey pk = CertificateUtil.parseRSAPublicKey(pem);
      assertNotNull(pk);
      assertEquals("RSA", pk.getAlgorithm());
    } catch (ServletException se) {
      fail("Should not have thrown ServletException");
    }
  }

}
