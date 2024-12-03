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
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer.SaslServerCallbackHandler;
import org.apache.hadoop.security.CustomizedCallbackHandler;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY;

/** For testing {@link CustomizedCallbackHandler}. */
public class TestCustomizedCallbackHandler {
  static final Logger LOG = LoggerFactory.getLogger(TestCustomizedCallbackHandler.class);

  static final AtomicReference<List<Callback>> LAST_CALLBACKS = new AtomicReference<>();

  static void reset() {
    LAST_CALLBACKS.set(null);
    CustomizedCallbackHandler.Cache.clear();
  }

  static void runHandleCallbacks(Object caller, List<Callback> callbacks, String name) {
    LOG.info("{}: handling {} for {}", caller.getClass().getSimpleName(), callbacks, name);
    LAST_CALLBACKS.set(callbacks);
  }

  /** Assert if the callbacks in {@link #LAST_CALLBACKS} are the same as the expected callbacks. */
  static void assertCallbacks(Callback[] expected) {
    final List<Callback> computed = LAST_CALLBACKS.getAndSet(null);
    Assert.assertNotNull(computed);
    Assert.assertEquals(expected.length, computed.size());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertSame(expected[i], computed.get(i));
    }
  }

  public static class MyCallback implements Callback { }

  public static class MyCallbackHandler implements CustomizedCallbackHandler {
    @Override
    public void handleCallbacks(List<Callback> callbacks, String name, char[] password) {
      runHandleCallbacks(this, callbacks, name);
    }
  }

  @Test
  public void testCustomizedCallbackHandler() throws Exception {
    final Configuration conf = new Configuration();
    final Callback[] callbacks = {new MyCallback()};

    // without setting conf, expect UnsupportedCallbackException
    reset();
    LambdaTestUtils.intercept(UnsupportedCallbackException.class, () -> runTest(conf, callbacks));

    // set conf and expect success
    reset();
    conf.setClass(HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY,
        MyCallbackHandler.class, CustomizedCallbackHandler.class);
    runTest(conf, callbacks);
    assertCallbacks(callbacks);

    reset();
    conf.setClass(HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY,
        MyCallbackHandler.class, CustomizedCallbackHandler.class);
    new SaslRpcServer.SaslDigestCallbackHandler(null, null, conf).handle(callbacks);
    assertCallbacks(callbacks);
  }

  public static class MyCallbackMethod {
    public void handleCallbacks(List<Callback> callbacks, String name, char[] password)
        throws UnsupportedCallbackException {
      runHandleCallbacks(this, callbacks, name);
    }
  }

  public static class MyExceptionMethod {
    public void handleCallbacks(List<Callback> callbacks, String name, char[] password)
        throws UnsupportedCallbackException {
      runHandleCallbacks(this, callbacks, name);
      throw new UnsupportedCallbackException(callbacks.get(0));
    }
  }

  @Test
  public void testCustomizedCallbackMethod() throws Exception {
    final Configuration conf = new Configuration();
    final Callback[] callbacks = {new MyCallback()};

    // without setting conf, expect UnsupportedCallbackException
    reset();
    LambdaTestUtils.intercept(UnsupportedCallbackException.class, () -> runTest(conf, callbacks));

    // set conf and expect success
    reset();
    conf.setClass(HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY,
        MyCallbackMethod.class, Object.class);
    runTest(conf, callbacks);
    assertCallbacks(callbacks);

    // set conf and expect exception
    reset();
    conf.setClass(HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY,
        MyExceptionMethod.class, Object.class);
    LambdaTestUtils.intercept(IOException.class, () -> runTest(conf, callbacks));
  }

  static void runTest(Configuration conf, Callback... callbacks)
      throws IOException, UnsupportedCallbackException {
    new SaslServerCallbackHandler(conf, String::toCharArray).handle(callbacks);
  }
}
