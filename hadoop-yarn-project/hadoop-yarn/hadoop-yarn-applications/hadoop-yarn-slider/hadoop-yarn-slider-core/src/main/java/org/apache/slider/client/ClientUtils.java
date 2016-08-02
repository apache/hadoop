/*
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
package org.apache.slider.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.NotFoundException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.core.registry.retrieve.RegistryRetriever;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.registry.client.binding.RegistryUtils.currentUser;
import static org.apache.hadoop.registry.client.binding.RegistryUtils.servicePath;

public class ClientUtils {
  public static ServiceRecord lookupServiceRecord(RegistryOperations rops,
      String user, String name) throws IOException, SliderException {
    return lookupServiceRecord(rops, user, null, name);
  }

  public static ServiceRecord lookupServiceRecord(RegistryOperations rops,
      String user, String type, String name) throws IOException,
      SliderException {
    if (StringUtils.isEmpty(user)) {
      user = currentUser();
    } else {
      user = RegistryPathUtils.encodeForRegistry(user);
    }
    if (StringUtils.isEmpty(type)) {
      type = SliderKeys.APP_TYPE;
    }

    String path = servicePath(user, type, name);
    return resolve(rops, path);
  }

  public static ServiceRecord resolve(RegistryOperations rops, String path)
      throws IOException, SliderException {
    try {
      return rops.resolve(path);
    } catch (PathNotFoundException | NoRecordException e) {
      throw new NotFoundException(e.getPath().toString(), e);
    }
  }

  public static PublishedConfiguration getConfigFromRegistry(
      RegistryOperations rops, Configuration configuration,
      String configName, String appName, String user, boolean external)
      throws IOException, SliderException {
    ServiceRecord instance = lookupServiceRecord(rops, user, appName);

    RegistryRetriever retriever = new RegistryRetriever(configuration, instance);
    PublishedConfigSet configurations = retriever.getConfigurations(external);

    PublishedConfiguration published = retriever.retrieveConfiguration(
        configurations, configName, external);
    return published;
  }

  public static String saveOrReturnConfig(PublishedConfiguration published,
      String format, File destPath, String fileName)
      throws BadCommandArgumentsException, IOException {
    ConfigFormat configFormat = ConfigFormat.resolve(format);
    if (configFormat == null) {
      throw new BadCommandArgumentsException(
          "Unknown/Unsupported format %s ", format);
    }
    PublishedConfigurationOutputter outputter =
        PublishedConfigurationOutputter.createOutputter(configFormat,
            published);
    boolean print = destPath == null;
    if (!print) {
      if (destPath.isDirectory()) {
        // creating it under a directory
        destPath = new File(destPath, fileName);
      }
      outputter.save(destPath);
      return null;
    } else {
      return outputter.asString();
    }
  }
}
