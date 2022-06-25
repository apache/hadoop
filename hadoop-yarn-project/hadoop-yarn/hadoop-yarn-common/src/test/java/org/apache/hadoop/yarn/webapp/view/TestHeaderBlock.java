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

package org.apache.hadoop.yarn.webapp.view;

import com.google.inject.Injector;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.yarn.webapp.test.WebAppTests;

import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestHeaderBlock {
  @SuppressWarnings("unchecked")
  public void setEnv(Map<String, String> newenv) throws Exception {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      String fieldname = "theEnvironment";
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField(fieldname);

      theEnvironmentField.setAccessible(true);
      if(theEnvironmentField.getType().equals(Map.class)) {
        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
        env.putAll(newenv);
      }
      fieldname = "theCaseInsensitiveEnvironment";
      Field theCaseInsensitiveEnvField = processEnvironmentClass.getDeclaredField(fieldname);

      theCaseInsensitiveEnvField.setAccessible(true);
      if(theCaseInsensitiveEnvField.getType().equals(Map.class)) {
        Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvField.get(null);
        cienv.putAll(newenv);
      }

    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for(Class cl : classes) {
        if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          if(field.getType().equals(Map.class)) {
            Map<String, String> map = (Map<String, String>) field.get(env);
            map.clear();
            map.putAll(newenv);
          }
        }
      }
    }
  }

  @Test public void testDefaultImgPath() throws Exception {
    Injector injector = WebAppTests.testBlock(HeaderBlock.class);
    PrintWriter out = injector.getInstance(PrintWriter.class);
    String expectation = " src=\"/static/hadoop-st.png\"";
    verify(out).print(expectation);
  }

  @Test public void testProxyBaseImgPath() throws Exception {
    String envkey = "APPLICATION_WEB_PROXY_BASE";
    String envvalue = "/hadoop";
    setEnv(Collections.singletonMap(envkey, envvalue));
    Injector injector = WebAppTests.testBlock(HeaderBlock.class);
    PrintWriter out = injector.getInstance(PrintWriter.class);
    String expectation = " src=\""+envvalue+"/static/hadoop-st.png\"";
    verify(out).print(expectation);
  }
}
