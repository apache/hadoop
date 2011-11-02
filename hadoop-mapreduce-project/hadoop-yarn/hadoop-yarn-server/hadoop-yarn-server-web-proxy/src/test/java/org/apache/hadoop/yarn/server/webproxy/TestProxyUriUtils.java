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

package org.apache.hadoop.yarn.server.webproxy;

import static org.junit.Assert.*;

import java.net.URI;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

public class TestProxyUriUtils {
  public static class TestAppId extends ApplicationId {
    private long timestamp;
    private int id;

    public TestAppId(int id, long timestamp) {
      setId(id);
      setClusterTimestamp(timestamp);
    }
    @Override
    public int getId() {
      return id;
    }

    @Override
    public void setId(int id) {
      this.id = id;
    }

    @Override
    public long getClusterTimestamp() {
      return timestamp;
    }

    @Override
    public void setClusterTimestamp(long clusterTimestamp) {
      this.timestamp = clusterTimestamp;
    }
  }
  
  @Test
  public void testGetPathApplicationId() {
    assertEquals("/proxy/application_100_0001", 
        ProxyUriUtils.getPath(new TestAppId(1, 100l)));
    assertEquals("/proxy/application_6384623_0005", 
        ProxyUriUtils.getPath(new TestAppId(5, 6384623l)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPathApplicationIdBad() {
    ProxyUriUtils.getPath(null);
  }
  
  @Test
  public void testGetPathApplicationIdString() {
    assertEquals("/proxy/application_6384623_0005", 
        ProxyUriUtils.getPath(new TestAppId(5, 6384623l), null));
    assertEquals("/proxy/application_6384623_0005/static/app",
        ProxyUriUtils.getPath(new TestAppId(5, 6384623l), "/static/app"));
    assertEquals("/proxy/application_6384623_0005/", 
        ProxyUriUtils.getPath(new TestAppId(5, 6384623l), "/"));
    assertEquals("/proxy/application_6384623_0005/some/path", 
        ProxyUriUtils.getPath(new TestAppId(5, 6384623l), "some/path"));
  }
  
  @Test 
  public void testGetPathAndQuery() {
    assertEquals("/proxy/application_6384623_0005/static/app?foo=bar",
    ProxyUriUtils.getPathAndQuery(new TestAppId(5, 6384623l), "/static/app", 
        "?foo=bar", false));
    
    assertEquals("/proxy/application_6384623_0005/static/app?foo=bar&bad=good&proxyapproved=true",
        ProxyUriUtils.getPathAndQuery(new TestAppId(5, 6384623l), "/static/app", 
            "foo=bar&bad=good", true));
  }

  @Test
  public void testGetProxyUri() throws Exception {
    URI originalUri = new URI("http://host.com/static/foo?bar=bar");
    URI proxyUri = new URI("http://proxy.net:8080/");
    TestAppId id = new TestAppId(5, 6384623l);
    URI expected = new URI("http://proxy.net:8080/proxy/application_6384623_0005/static/foo?bar=bar");
    URI result = ProxyUriUtils.getProxyUri(originalUri, proxyUri, id);
    assertEquals(expected, result);
  }

}
