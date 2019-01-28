/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.conf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.LambdaTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the UnmodifiableConfiguration.
 */
public class TestUnmodifiableConfiguration {

  private static Logger log = LoggerFactory
          .getLogger(TestUnmodifiableConfiguration.class);
  private static Configuration hadoopConf = new Configuration();

  private enum TEST {
    BLAH
  }

  @Test
  public void testGet() {
    Configuration conf = Configuration.unmodifiableConfiguration(hadoopConf);
    String prop = conf.get("blah", "blah");
    assertEquals("blah", prop);
  }

  @Test
  public void testIter() throws Exception{
    Configuration conf = Configuration.unmodifiableConfiguration(hadoopConf);
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        () -> conf.iterator().remove());
  }

  @Test
  /**
   * Test that all mutable methods throw exceptions. Only acceptable methods:
   *
   * <pre>
   * get*
   * onlyKeyExists()
   * is*
   * iterator()
   * size()
   * toString()
   * </pre>
   */
  public void testAllMethods() throws Exception {
    UnmodifiableConfiguration conf = new UnmodifiableConfiguration(hadoopConf);
    Method[] allMethods = UnmodifiableConfiguration.class.getDeclaredMethods();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(0);
    ByteArrayInputStream bais = new ByteArrayInputStream("".getBytes());

    for (Method method : allMethods) {
      String name = method.getName();
      if (!name.startsWith("get") && !name.startsWith("i") &&
              !name.equals("onlyKeyExists") && !name.equals("toString") &&
              !name.equals("size")) {
        log.info("Testing method {}", method.getName());

        // get the proper params for the method
        List<Object> params = new ArrayList<>();
        for (Class<?> param : method.getParameterTypes()) {
          log.debug("Method has paramType {}", param.getName());
          if (param.isPrimitive()) {
            if (param.getName().equals("boolean")) {
              params.add(Boolean.TRUE);
            } else {
              params.add(0);
            }
            continue;
          }
          if (param.isArray()) {
            params.add(new String[] {});
            continue;
          }
          switch (param.getName()) {
          case "java.lang.String":
          case "org.apache.hadoop.fs.Path":
          case "java.net.URL":
            params.add(param.getDeclaredConstructor(String.class)
                    .newInstance("http://"));
            break;
          case "java.io.DataOutput":
          case "java.io.OutputStream":
            params.add(new DataOutputStream(baos));
            break;
          case "java.io.DataInput":
          case "java.io.InputStream":
            params.add(new DataInputStream(bais));
            break;
          case "java.util.regex.Pattern":
            params.add(java.util.regex.Pattern.compile("b"));
            break;
          case "java.lang.Enum":
            params.add(TEST.BLAH);
            break;
          case "java.util.concurrent.TimeUnit":
            params.add(TimeUnit.DAYS);
            break;
          case "org.apache.hadoop.conf.StorageUnit":
            params.add(StorageUnit.GB);
            break;
          case "java.net.InetSocketAddress":
            params.add(new java.net.InetSocketAddress(1));
            break;
          case "java.lang.Class":
            params.add(this.getClass());
            break;
          case "java.io.Writer":
            params.add(new StringWriter(0));
            break;
          case "java.lang.ClassLoader":
            params.add(ClassLoader.getSystemClassLoader());
            break;
          default:
            params.add(param.newInstance());
            break;
          }
        }
        try {
          log.info("invoke {} with {} params: {}", name, params.size(),
                  params);
          callMethod(conf, method, params);
          fail("Method did not throw UnsupportedOperationException: " +
                  method.getName());
        } catch (InvocationTargetException e) {
          if (!(e.getCause() instanceof UnsupportedOperationException)) {
            throw e;
          }
        }
      }
    }
  }

  private void callMethod(Configuration conf, Method method, List<?> params)
          throws Exception {
    switch (params.size()) {
    case 1:
      method.invoke(conf, params.get(0));
      break;
    case 2:
      method.invoke(conf, params.get(0), params.get(1));
      break;
    case 3:
      method.invoke(conf, params.get(0), params.get(1), params.get(2));
      break;
    case 4:
      method.invoke(conf, params.get(0), params.get(1), params.get(2),
              params.get(3));
      break;
    default:
      method.invoke(conf);
    }
  }
}
