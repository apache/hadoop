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

import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;

public class TestBasicAuthConfigurator {
  @Test
  public void testNullCredentials() throws IOException {
    ConnectionConfigurator conf = new BasicAuthConfigurator(null, null);
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    conf.configure(conn);
    Mockito.verify(conn, Mockito.never()).setRequestProperty(Mockito.any(), Mockito.any());
  }

  @Test
  public void testEmptyCredentials() throws IOException {
    ConnectionConfigurator conf = new BasicAuthConfigurator(null, "");
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    conf.configure(conn);
    Mockito.verify(conn, Mockito.never()).setRequestProperty(Mockito.any(), Mockito.any());
  }

  @Test
  public void testCredentialsSet() throws IOException {
    ConnectionConfigurator conf = new BasicAuthConfigurator(null, "user:pass");
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    conf.configure(conn);
    Mockito.verify(conn, Mockito.times(1)).setRequestProperty(
        "AUTHORIZATION",
        "Basic dXNlcjpwYXNz"
    );
  }

  @Test
  public void testParentConfigurator() throws IOException {
    ConnectionConfigurator parent = Mockito.mock(ConnectionConfigurator.class);
    ConnectionConfigurator conf = new BasicAuthConfigurator(parent, "user:pass");
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    conf.configure(conn);
    Mockito.verify(conn, Mockito.times(1)).setRequestProperty(
        "AUTHORIZATION",
        "Basic dXNlcjpwYXNz"
    );
    Mockito.verify(parent, Mockito.times(1)).configure(conn);
  }
}
