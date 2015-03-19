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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX_LENGTH;

class ParameterParser {
  private final Configuration conf;
  private final String path;
  private final Map<String, List<String>> params;

  ParameterParser(QueryStringDecoder decoder, Configuration conf) {
    this.path = decodeComponent(decoder.path().substring
        (WEBHDFS_PREFIX_LENGTH), Charsets.UTF_8);
    this.params = decoder.parameters();
    this.conf = conf;
  }

  String path() { return path; }

  String op() {
    return param(HttpOpParam.NAME);
  }

  long offset() {
    return new OffsetParam(param(OffsetParam.NAME)).getOffset();
  }

  long length() {
    return new LengthParam(param(LengthParam.NAME)).getLength();
  }

  String namenodeId() {
    return new NamenodeAddressParam(param(NamenodeAddressParam.NAME))
      .getValue();
  }

  String doAsUser() {
    return new DoAsParam(param(DoAsParam.NAME)).getValue();
  }

  String userName() {
    return new UserParam(param(UserParam.NAME)).getValue();
  }

  int bufferSize() {
    return new BufferSizeParam(param(BufferSizeParam.NAME)).getValue(conf);
  }

  long blockSize() {
    return new BlockSizeParam(param(BlockSizeParam.NAME)).getValue(conf);
  }

  short replication() {
    return new ReplicationParam(param(ReplicationParam.NAME)).getValue(conf);
  }

  FsPermission permission() {
    return new PermissionParam(param(PermissionParam.NAME)).getFsPermission();
  }

  boolean overwrite() {
    return new OverwriteParam(param(OverwriteParam.NAME)).getValue();
  }

  Token<DelegationTokenIdentifier> delegationToken() throws IOException {
    String delegation = param(DelegationParam.NAME);
    final Token<DelegationTokenIdentifier> token = new
      Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(delegation);
    URI nnUri = URI.create(HDFS_URI_SCHEME + "://" + namenodeId());
    boolean isLogical = HAUtil.isLogicalUri(conf, nnUri);
    if (isLogical) {
      token.setService(HAUtil.buildTokenServiceForLogicalUri(nnUri,
        HDFS_URI_SCHEME));
    } else {
      token.setService(SecurityUtil.buildTokenService(nnUri));
    }
    return token;
  }

  Configuration conf() {
    return conf;
  }

  private String param(String key) {
    List<String> p = params.get(key);
    return p == null ? null : p.get(0);
  }

  /**
   * The following function behaves exactly the same as netty's
   * <code>QueryStringDecoder#decodeComponent</code> except that it
   * does not decode the '+' character as space. WebHDFS takes this scheme
   * to maintain the backward-compatibility for pre-2.7 releases.
   */
  private static String decodeComponent(final String s, final Charset charset) {
    if (s == null) {
      return "";
    }
    final int size = s.length();
    boolean modified = false;
    for (int i = 0; i < size; i++) {
      final char c = s.charAt(i);
      if (c == '%' || c == '+') {
        modified = true;
        break;
      }
    }
    if (!modified) {
      return s;
    }
    final byte[] buf = new byte[size];
    int pos = 0;  // position in `buf'.
    for (int i = 0; i < size; i++) {
      char c = s.charAt(i);
      if (c == '%') {
        if (i == size - 1) {
          throw new IllegalArgumentException("unterminated escape sequence at" +
                                                 " end of string: " + s);
        }
        c = s.charAt(++i);
        if (c == '%') {
          buf[pos++] = '%';  // "%%" -> "%"
          break;
        }
        if (i == size - 1) {
          throw new IllegalArgumentException("partial escape sequence at end " +
                                                 "of string: " + s);
        }
        c = decodeHexNibble(c);
        final char c2 = decodeHexNibble(s.charAt(++i));
        if (c == Character.MAX_VALUE || c2 == Character.MAX_VALUE) {
          throw new IllegalArgumentException(
              "invalid escape sequence `%" + s.charAt(i - 1) + s.charAt(
                  i) + "' at index " + (i - 2) + " of: " + s);
        }
        c = (char) (c * 16 + c2);
        // Fall through.
      }
      buf[pos++] = (byte) c;
    }
    return new String(buf, 0, pos, charset);
  }

  /**
   * Helper to decode half of a hexadecimal number from a string.
   * @param c The ASCII character of the hexadecimal number to decode.
   * Must be in the range {@code [0-9a-fA-F]}.
   * @return The hexadecimal value represented in the ASCII character
   * given, or {@link Character#MAX_VALUE} if the character is invalid.
   */
  private static char decodeHexNibble(final char c) {
    if ('0' <= c && c <= '9') {
      return (char) (c - '0');
    } else if ('a' <= c && c <= 'f') {
      return (char) (c - 'a' + 10);
    } else if ('A' <= c && c <= 'F') {
      return (char) (c - 'A' + 10);
    } else {
      return Character.MAX_VALUE;
    }
  }
}
