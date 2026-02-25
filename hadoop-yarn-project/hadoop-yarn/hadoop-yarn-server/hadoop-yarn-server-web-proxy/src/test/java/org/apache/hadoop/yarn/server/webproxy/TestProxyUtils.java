/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.http.client.methods.HttpRequestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestProxyUtils {

  @Test
  void testGetCookie() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    Cookie[] cookies = {
        new Cookie("foo", "foo_value"),
        new Cookie("bar", "222")
    };
    when(req.getCookies()).thenReturn(cookies);
    assertEquals("foo_value", ProxyUtils.getCookie(req, "foo"));
    assertEquals("222", ProxyUtils.getCookie(req, "bar"));
    assertNull(ProxyUtils.getCookie(req, "baz"));
  }

  @Test
  void testSetCookie() {
    HttpRequestBase mock = mock(HttpRequestBase.class);
    Map<String, String> cookies = new TreeMap<>();
    cookies.put("foo", "foo_value");
    cookies.put("bar", "222");
    ProxyUtils.setCookies(mock, cookies);
    ArgumentCaptor<String> headerCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
    verify(mock, times(1))
        .setHeader(headerCaptor.capture(), valueCaptor.capture());
    assertEquals("Cookie", headerCaptor.getValue());
    assertEquals("bar=222; foo=foo_value", valueCaptor.getValue());
  }

  @Test
  void testSetEmptyCookie() {
    HttpRequestBase mock = mock(HttpRequestBase.class);
    ProxyUtils.setCookies(mock, new HashMap<>());
    verify(mock, never()).setHeader(anyString(), anyString());
  }


  @Test
  void testSetNullCookie() {
    HttpRequestBase mock = mock(HttpRequestBase.class);
    ProxyUtils.setCookies(mock, null);
    verify(mock, never()).setHeader(anyString(), anyString());
  }
}
