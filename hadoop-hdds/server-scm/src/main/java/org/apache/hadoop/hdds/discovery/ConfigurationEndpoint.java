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
package org.apache.hadoop.hdds.discovery;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManagerHttpServer;

/**
 * JAXRS endpoint to publish current ozone configuration.
 */
@Path("/config")
public class ConfigurationEndpoint {

  private Properties defaults =
      OzoneConfiguration.createWithDefaultsOnly().getProps();

  @javax.ws.rs.core.Context
  private ServletContext context;

  /**
   * Returns with the non-default configuration.
   */
  @GET
  public ConfigurationXml getConfiguration() {
    OzoneConfiguration conf = (OzoneConfiguration) context.getAttribute(
        StorageContainerManagerHttpServer.CONFIG_CONTEXT_ATTRIBUTE);
    ConfigurationXml configXml = new ConfigurationXml();
    for (Entry<Object, Object> entry : conf.getProps().entrySet()) {
      //return only the non-defaults
      if (defaults.get(entry.getKey()) != entry.getValue()) {
        configXml.addConfiguration(entry.getKey().toString(),
            entry.getValue().toString());
      }
    }
    return configXml;
  }

}
