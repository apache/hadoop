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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.util.EnumSet;


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
    Assert.assertTrue(HAUtilClient.isTokenForLogicalUri(tok2));
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
  public void testCreateFlag() {
    final String path = "/test1?createflag=append,sync_block";
    Configuration conf = new Configuration();
    QueryStringDecoder decoder = new QueryStringDecoder(
        WebHdfsHandler.WEBHDFS_PREFIX + path);
    ParameterParser testParser = new ParameterParser(decoder, conf);
    EnumSet<CreateFlag> actual = testParser.createFlag();
    EnumSet<CreateFlag> expected = EnumSet.of(CreateFlag.APPEND,
        CreateFlag.SYNC_BLOCK);
    Assert.assertEquals(expected.toString(), actual.toString());


    final String path1 = "/test1?createflag=append";
    decoder = new QueryStringDecoder(
        WebHdfsHandler.WEBHDFS_PREFIX + path1);
    testParser = new ParameterParser(decoder, conf);

    actual = testParser.createFlag();
    expected = EnumSet.of(CreateFlag.APPEND);
    Assert.assertEquals(expected, actual);

    final String path2 = "/test1";
    decoder = new QueryStringDecoder(
        WebHdfsHandler.WEBHDFS_PREFIX + path2);
    testParser = new ParameterParser(decoder, conf);
    actual = testParser.createFlag();
    Assert.assertEquals(0, actual.size());

    final String path3 = "/test1?createflag=create,overwrite";
    decoder = new QueryStringDecoder(
        WebHdfsHandler.WEBHDFS_PREFIX + path3);
    testParser = new ParameterParser(decoder, conf);
    actual = testParser.createFlag();
    expected = EnumSet.of(CreateFlag.CREATE, CreateFlag
        .OVERWRITE);
    Assert.assertEquals(expected.toString(), actual.toString());


    final String path4 = "/test1?createflag=";
    decoder = new QueryStringDecoder(
        WebHdfsHandler.WEBHDFS_PREFIX + path4);
    testParser = new ParameterParser(decoder, conf);
    actual = testParser.createFlag();
    Assert.assertEquals(0, actual.size());

    //Incorrect value passed to createflag
    try {
      final String path5 = "/test1?createflag=overwrite,";
      decoder = new QueryStringDecoder(
          WebHdfsHandler.WEBHDFS_PREFIX + path5);
      testParser = new ParameterParser(decoder, conf);
      actual = testParser.createFlag();
      fail("It should throw Illegal Argument Exception");
    } catch (Exception e) {
      GenericTestUtils
          .assertExceptionContains("No enum constant", e);
    }

    //Incorrect value passed to createflag
    try {
      final String path6 = "/test1?createflag=,";
      decoder = new QueryStringDecoder(
          WebHdfsHandler.WEBHDFS_PREFIX + path6);
      testParser = new ParameterParser(decoder, conf);
      actual = testParser.createFlag();
      fail("It should throw Illegal Argument Exception");
    } catch (Exception e) {
      GenericTestUtils
          .assertExceptionContains("No enum constant", e);
    }


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
