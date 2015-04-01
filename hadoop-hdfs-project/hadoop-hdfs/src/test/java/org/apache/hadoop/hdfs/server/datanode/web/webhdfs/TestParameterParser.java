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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;

import io.netty.handler.codec.http.QueryStringDecoder;

import javax.servlet.ServletContext;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestParameterParser {
  private static final String LOGICAL_NAME = "minidfs";

  @Test
  public void testDeserializeHAToken() throws IOException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    final Token<DelegationTokenIdentifier> token = new
        Token<DelegationTokenIdentifier>();
    QueryStringDecoder decoder = new QueryStringDecoder(
      WebHdfsHandler.WEBHDFS_PREFIX + "/?"
      + NamenodeAddressParam.NAME + "=" + LOGICAL_NAME + "&"
      + DelegationParam.NAME + "=" + token.encodeToUrlString());
    ParameterParser testParser = new ParameterParser(decoder, conf);
    final Token<DelegationTokenIdentifier> tok2 = testParser.delegationToken();
    Assert.assertTrue(HAUtil.isTokenForLogicalUri(tok2));
  }

  @Test
  public void testDecodePath() {
    final String ESCAPED_PATH = "/test%25+1%26%3Dtest?op=OPEN&foo=bar";
    final String EXPECTED_PATH = "/test%+1&=test";

    Configuration conf = new Configuration();
    QueryStringDecoder decoder = new QueryStringDecoder(
      WebHdfsHandler.WEBHDFS_PREFIX + ESCAPED_PATH);
    ParameterParser testParser = new ParameterParser(decoder, conf);
    Assert.assertEquals(EXPECTED_PATH, testParser.path());
  }

  @Test
  public void testOffset() throws IOException {
    final long X = 42;

    long offset = new OffsetParam(Long.toString(X)).getOffset();
    Assert.assertEquals("OffsetParam: ", X, offset);

    offset = new OffsetParam((String) null).getOffset();
    Assert.assertEquals("OffsetParam with null should have defaulted to 0", 0, offset);

    try {
      offset = new OffsetParam("abc").getValue();
      Assert.fail("OffsetParam with nondigit value should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Ignore
    }
  }
}
