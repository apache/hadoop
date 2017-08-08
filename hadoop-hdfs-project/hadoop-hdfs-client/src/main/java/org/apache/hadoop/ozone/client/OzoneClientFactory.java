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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.client.rest.OzoneRestClient;
import org.apache.hadoop.ozone.client.rpc.OzoneRpcClient;

import java.io.IOException;

/**
 * Factory class to create different types of OzoneClients.
 */
public final class OzoneClientFactory {

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private OzoneClientFactory(){}

  private static Configuration configuration;

  /**
   * Returns an OzoneClient which will use RPC protocol to perform
   * client operations.
   *
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getClient() throws IOException {
    //TODO: get client based on ozone.client.protocol
    return new OzoneRpcClient(getConfiguration());
  }

  /**
   * Returns an OzoneClient which will use RPC protocol to perform
   * client operations.
   *
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getRpcClient() throws IOException {
    return new OzoneRpcClient(getConfiguration());
  }

  /**
   * Returns an OzoneClient which will use RPC protocol to perform
   * client operations.
   *
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getRestClient() throws IOException {
    return new OzoneRestClient(getConfiguration());
  }

  /**
   * Sets the configuration, which will be used while creating OzoneClient.
   *
   * @param conf
   */
  public static void setConfiguration(Configuration conf) {
    configuration = conf;
  }

  /**
   * Returns the configuration if it's already set, else creates a new
   * {@link OzoneConfiguration} and returns it.
   *
   * @return Configuration
   */
  private static synchronized Configuration getConfiguration() {
    if(configuration == null) {
      setConfiguration(new OzoneConfiguration());
    }
    return configuration;
  }
}
