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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.View;

import static org.mockito.Mockito.*;

/**
 * Class AppControllerForTest overrides some methods of AppController for test
 */
public class AppControllerForTest extends AppController {
  final private static Map<String, String> properties = new HashMap<String, String>();

  private ResponseInfo responseInfo = new ResponseInfo();
  private View view = new ViewForTest();
  private Class<?> clazz;
  private HttpServletResponse response;


  protected AppControllerForTest(App app, Configuration configuration, RequestContext ctx) {
    super(app, configuration, ctx);
  }

  public Class<?> getClazz() {
    return clazz;
  }

  @SuppressWarnings("unchecked")
  public <T> T getInstance(Class<T> cls) {
    clazz = cls;
    if (cls.equals(ResponseInfo.class)) {
      return (T) responseInfo;
    }
    return (T) view;
  }

  public ResponseInfo getResponseInfo() {
    return responseInfo;
  }

  public String get(String key, String defaultValue) {
    String result = properties.get(key);
    if (result == null) {
      result = defaultValue;
    }
    return result;
  }

  public void set(String key, String value) {
    properties.put(key, value);
  }

  public HttpServletRequest request() {
    HttpServletRequest result = mock(HttpServletRequest.class);
    when(result.getRemoteUser()).thenReturn("user");


    return result;
  }

  @Override
  public HttpServletResponse response() {
    if (response == null) {
      response = mock(HttpServletResponse.class);
    }
    return response;
  }

  public Map<String, String> getProperty() {
    return properties;
  }

  OutputStream data = new ByteArrayOutputStream();
  PrintWriter writer = new PrintWriter(data);

  public String getData() {
    writer.flush();
    return data.toString();
  }

  protected PrintWriter writer() {
    if (writer == null) {
      writer = new PrintWriter(data);
    }
    return writer;
  }
}
