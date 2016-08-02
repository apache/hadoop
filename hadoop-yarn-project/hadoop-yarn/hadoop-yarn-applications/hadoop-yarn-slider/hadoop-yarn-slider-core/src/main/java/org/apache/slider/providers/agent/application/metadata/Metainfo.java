/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Application metainfo uber class
 */
public class Metainfo {
  protected static final Logger log =
      LoggerFactory.getLogger(Metainfo.class);
  public static String VERSION_TWO_ZERO = "2.0";
  public static String VERSION_TWO_ONE = "2.1";

  String schemaVersion;
  ApplicationPackage applicationPackage;
  Application application;

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public ApplicationPackage getApplicationPackage() {
    return applicationPackage;
  }

  public void setApplicationPackage(ApplicationPackage pkg) {
    this.applicationPackage = pkg;
  }

  public Application getApplication() {
    return application;
  }

  public void setApplication(Application application) {
    this.application = application;
  }

  public Component getApplicationComponent(String roleGroup) {
    if (application == null) {
      log.error("Malformed app definition: Expect application as the top level element for metainfo");
    } else {
      for (Component component : application.getComponents()) {
        if (component.getName().equals(roleGroup)) {
          return component;
        }
      }
    }
    return null;
  }

  public List<ConfigFile> getComponentConfigFiles(String roleGroup) {
    List<ConfigFile> componentConfigFiles = new ArrayList<>();
    componentConfigFiles.addAll(application.getConfigFiles());
    Component component = getApplicationComponent(roleGroup);
    if (component != null) {
      componentConfigFiles.addAll(component.getConfigFiles());
    }
    return componentConfigFiles;
  }

  public void validate() throws SliderException {
    if (!VERSION_TWO_ONE.equals(schemaVersion) &&
        !VERSION_TWO_ZERO.equals(schemaVersion)) {
      throw new SliderException("Unsupported version " + getSchemaVersion());
    }
    if (application != null) {
      application.validate(schemaVersion);
    }
    if (applicationPackage != null) {
      applicationPackage.validate(schemaVersion);
    }
  }

  public static void checkNonNull(String value, String field, String type) throws SliderException {
    if (SliderUtils.isUnset(value)) {
      throw new SliderException(type + "." + field + " cannot be null");
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Metainfo [schemaVersion=");
    builder.append(schemaVersion);
    builder.append(", applicationPackage=");
    builder.append(applicationPackage);
    builder.append(", application=");
    builder.append(application);
    builder.append("]");
    return builder.toString();
  }
}
