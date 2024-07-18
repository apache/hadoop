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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer.SaslServerCallbackHandler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.util.Arrays;
import java.util.List;

public class TestCustomizedCallbackHandler {
  public static final Logger LOG = LoggerFactory.getLogger(TestCustomizedCallbackHandler.class);

  static class MyCallback implements Callback { }

  static class MyCallbackHandler implements CustomizedCallbackHandler {
    @Override
    public void handleCallback(List<Callback> callbacks, String name, char[] password) {
      LOG.info("{}: handling {} for {}", getClass().getSimpleName(), callbacks, name);
    }
  }

  @Test
  public void testCustomizedCallbackHandler() throws Exception {
    final Configuration conf = new Configuration();
    final Callback[] callbacks = {new MyCallback()};

    // without setting conf, expect UnsupportedCallbackException
    try {
      new SaslServerCallbackHandler(conf, String::toCharArray).handle(callbacks);
      Assert.fail("Expected UnsupportedCallbackException for " + Arrays.asList(callbacks));
    } catch (UnsupportedCallbackException e) {
      LOG.info("The failure is expected", e);
    }

    // set conf and expect success
    conf.setClass(HdfsClientConfigKeys.DFS_DATA_TRANSFER_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY,
        MyCallbackHandler.class, CustomizedCallbackHandler.class);
    new SaslServerCallbackHandler(conf, String::toCharArray).handle(callbacks);
  }
}
