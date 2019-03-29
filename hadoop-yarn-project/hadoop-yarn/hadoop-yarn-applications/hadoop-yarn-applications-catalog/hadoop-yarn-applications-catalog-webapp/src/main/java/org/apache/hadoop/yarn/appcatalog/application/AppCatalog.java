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

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.hadoop.yarn.appcatalog.controller.AppDetailsController;

import java.util.Set;

/**
 * Jackson resource configuration class for Application Catalog.
 */
@ApplicationPath("service")
public class AppCatalog extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> resources = new java.util.HashSet<>();
    // following code can be used to customize Jersey 2.0 JSON provider:
    try {
      final Class<?> jsonProvider =
          Class.forName("org.glassfish.jersey.jackson.JacksonFeature");
      // Class jsonProvider =
      // Class.forName("org.glassfish.jersey.moxy.json.MoxyJsonFeature");
      // Class jsonProvider =
      // Class.forName("org.glassfish.jersey.jettison.JettisonFeature");
      resources.add(jsonProvider);
    } catch (final ClassNotFoundException ex) {
      ex.printStackTrace();
    }
    addRestResourceClasses(resources);
    return resources;
  }

  /**
   * Add your own resources here.
   */
  private void addRestResourceClasses(final Set<Class<?>> resources) {
    resources.add(AppDetailsController.class);
  }

}
