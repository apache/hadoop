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

package org.apache.hadoop.lib.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.HTestCase;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBaseService extends HTestCase {

  public static class MyService extends BaseService {
    static Boolean INIT;

    public MyService() {
      super("myservice");
    }

    @Override
    protected void init() throws ServiceException {
      INIT = true;
    }

    @Override
    public Class getInterface() {
      return null;
    }
  }

  @Test
  public void baseService() throws Exception {
    BaseService service = new MyService();
    assertNull(service.getInterface());
    assertEquals(service.getPrefix(), "myservice");
    assertEquals(service.getServiceDependencies().length, 0);

    Server server = Mockito.mock(Server.class);
    Configuration conf = new Configuration(false);
    conf.set("server.myservice.foo", "FOO");
    conf.set("server.myservice1.bar", "BAR");
    Mockito.when(server.getConfig()).thenReturn(conf);
    Mockito.when(server.getPrefixedName("myservice.foo")).thenReturn("server.myservice.foo");
    Mockito.when(server.getPrefixedName("myservice.")).thenReturn("server.myservice.");

    service.init(server);
    assertEquals(service.getPrefixedName("foo"), "server.myservice.foo");
    assertEquals(service.getServiceConfig().size(), 1);
    assertEquals(service.getServiceConfig().get("foo"), "FOO");
    assertTrue(MyService.INIT);
  }
}
