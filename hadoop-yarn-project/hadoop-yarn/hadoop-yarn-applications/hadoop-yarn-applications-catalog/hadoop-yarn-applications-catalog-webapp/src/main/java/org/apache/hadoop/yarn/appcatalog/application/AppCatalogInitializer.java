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
package org.apache.hadoop.yarn.appcatalog.application;

import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.HadoopKerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initialization class for setting Kerberos configuration.
 */
public class AppCatalogInitializer implements ServletContextListener {

  static final Logger LOG = LoggerFactory.getLogger(
      AppCatalogInitializer.class);

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    Configuration conf = new Configuration();
    if (!HadoopKerberosName.hasRulesBeenSet()) {
      try {
        HadoopKerberosName.setConfiguration(conf);
      } catch (IOException e) {
        LOG.error("Application Catalog initialization failed:", e);
      }
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
  }

}
