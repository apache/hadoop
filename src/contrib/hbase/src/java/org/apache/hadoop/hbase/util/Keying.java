/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;

/**
 * Utility creating hbase friendly keys.
 * Use fabricating row names or column qualifiers.
 * <p>TODO: Add createSchemeless key, a key that doesn't care if scheme is
 * http or https.
 */
public class Keying {
  private static final String SCHEME = "r:";
  private static final Pattern URI_RE_PARSER =
    Pattern.compile("^([^:/?#]+://(?:[^/?#@]+@)?)([^:/?#]+)(.*)$");

  /**
   * Makes a key out of passed URI for use as row name or column qualifier.
   * 
   * This method runs transforms on the passed URI so it sits better
   * as a key (or portion-of-a-key) in hbase.  The <code>host</code> portion of
   * the URI authority is reversed so subdomains sort under their parent
   * domain.  The returned String is an opaque URI of an artificial
   * <code>r:</code> scheme to prevent the result being considered an URI of
   * the original scheme.  Here is an example of the transform: The url
   * <code>http://lucene.apache.org/index.html?query=something#middle<code> is
   * returned as
   * <code>r:http://org.apache.lucene/index.html?query=something#middle</code>
   * The transforms are reversible.  No transform is done if passed URI is
   * not hierarchical.
   * 
   * <p>If authority <code>userinfo</code> is present, will mess up the sort
   * (until we do more work).</p>
   * 
   * @param u URL to transform.
   * @return An opaque URI of artificial 'r' scheme with host portion of URI
   * authority reversed (if present).
   * @see #keyToUri(String)
   * @see <a href="http://www.ietf.org/rfc/rfc2396.txt">RFC2396</a>
   */
  public static String createKey(final String u) {
    if (u.startsWith(SCHEME)) {
      throw new IllegalArgumentException("Starts with " + SCHEME);
    }
    Matcher m = getMatcher(u);
    if (m == null || !m.matches()) {
      // If no match, return original String.
      return u;
    }
    return SCHEME + m.group(1) + reverseHostname(m.group(2)) + m.group(3);
  }
  
  /**
   * Reverse the {@link #createKey(String)} transform.
   * 
   * @param s <code>URI</code> made by {@link #createKey(String)}.
   * @return 'Restored' URI made by reversing the {@link #createKey(String)}
   * transform.
   */
  public static String keyToUri(final String s) {
    if (!s.startsWith(SCHEME)) {
      return s;
    }
    Matcher m = getMatcher(s.substring(SCHEME.length()));
    if (m == null || !m.matches()) {
      // If no match, return original String.
      return s;
    }
    return m.group(1) + reverseHostname(m.group(2)) + m.group(3);
  }
  
  private static Matcher getMatcher(final String u) {
    if (u == null || u.length() <= 0) {
      return null;
    }
    return URI_RE_PARSER.matcher(u);
  }
  
  private static String reverseHostname(final String hostname) {
    if (hostname == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(hostname.length());
    for (StringTokenizer st = new StringTokenizer(hostname, ".", false);
        st.hasMoreElements();) {
      Object next = st.nextElement();
      if (sb.length() > 0) {
        sb.insert(0, ".");
      }
      sb.insert(0, next);
    }
    return sb.toString();
  }
  
  /**
   * @param i
   * @return <code>i</code> as byte array.
   */
  public static byte[] intToBytes(final int i){
    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
    buffer.putInt(i);
    return buffer.array();
  }
  
  /**
   * @param l
   * @return <code>i</code> as byte array.
   */
  public static byte[] longToBytes(final long l){
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
    buffer.putLong(l);
    return buffer.array();
  }

  /**
   * Returns row and column bytes out of an HStoreKey.
   * @param hsk Store key.
   * @throws UnsupportedEncodingException
   */
  public static byte[] getBytes(final HStoreKey hsk)
  throws UnsupportedEncodingException {
    StringBuilder s = new StringBuilder(hsk.getRow().toString());
    s.append(hsk.getColumn().toString());
    return s.toString().getBytes(HConstants.UTF8_ENCODING);
  }

  /**
   * @param bytes
   * @return String made of the bytes or null if bytes are null.
   * @throws UnsupportedEncodingException
   */
  public static String bytesToString(final byte [] bytes)
  throws UnsupportedEncodingException {
    if(bytes == null) {
      return null;
    }
    return new String(bytes, HConstants.UTF8_ENCODING);
  }
  
  public static long bytesToLong(final byte [] bytes) throws IOException {
    long result = -1;
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new ByteArrayInputStream(bytes));
      result = dis.readLong();
    } finally {
      dis.close();
    }
    return result;
  }
}