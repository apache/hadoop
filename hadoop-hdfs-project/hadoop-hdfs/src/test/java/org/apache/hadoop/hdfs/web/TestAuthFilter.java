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
package org.apache.hadoop.hdfs.web;


import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.junit.Test;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAuthFilter {

  private static final String PREFIX = "hadoop.http.authentication.";

  @Test
  public void testGetConfiguration() {
    Configuration conf = new Configuration();
    conf.set(PREFIX + "type", "kerberos");
    conf.set(PREFIX + "kerberos.keytab", "thekeytab");
    conf.set(PREFIX + "kerberos.principal", "xyz/thehost@REALM");

    FilterContainer container = Mockito.mock(FilterContainer.class);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        Object[] args = invocationOnMock.getArguments();

        assertEquals("AuthFilter", args[0]);
        assertEquals(AuthFilter.class.getName(), args[1]);

        Map<String, String> conf = (Map<String, String>) args[2];
        assertEquals("/", conf.get("cookie.path"));
        assertEquals("kerberos", conf.get("type"));
        assertNull(conf.get("cookie.domain"));
        assertEquals("xyz/thehost@REALM", conf.get("kerberos.principal"));
        assertEquals("thekeytab", conf.get("kerberos.keytab"));
        assertEquals("true",
            conf.get(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));

        return null;
      }
    }).when(container).addFilter(Mockito.any(), Mockito.any(), Mockito.any());

    new AuthFilterInitializer().initFilter(container, conf);
  }


}
