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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;

/**
 * Custom implementation of {@link ManagedHttpClientConnectionFactory} and overrides
 * {@link ManagedHttpClientConnectionFactory#create(HttpRoute, ConnectionConfig)} to return
 * {@link AbfsManagedApacheHttpConnection}.
 */
public class AbfsHttpClientConnectionFactory extends ManagedHttpClientConnectionFactory {

  /**
   * Creates a new {@link AbfsManagedApacheHttpConnection} instance which has to
   * be connected.
   * @param route route for which connection is required.
   * @param config connection configuration.
   * @return new {@link AbfsManagedApacheHttpConnection} instance.
   */
  @Override
  public ManagedHttpClientConnection create(final HttpRoute route,
      final ConnectionConfig config) {
    return new AbfsManagedApacheHttpConnection(super.create(route, config), route);
  }
}
