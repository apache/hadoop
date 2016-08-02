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
package org.apache.slider.server.appmaster.security;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import static org.apache.slider.core.main.LauncherExitCodes.EXIT_UNAUTHORIZED;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Class keeping code security information
 */
public class SecurityConfiguration {

  protected static final Logger log =
      LoggerFactory.getLogger(SecurityConfiguration.class);
  private final Configuration configuration;
  private final AggregateConf instanceDefinition;
  private String clusterName;

  public SecurityConfiguration(Configuration configuration,
                               AggregateConf instanceDefinition,
                               String clusterName) throws SliderException {
    Preconditions.checkNotNull(configuration);
    Preconditions.checkNotNull(instanceDefinition);
    Preconditions.checkNotNull(clusterName);
    this.configuration = configuration;
    this.instanceDefinition = instanceDefinition;
    this.clusterName = clusterName;
    validate();
  }

  private void validate() throws SliderException {
    if (isSecurityEnabled()) {
      String principal = instanceDefinition.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM).get(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL);
      if(SliderUtils.isUnset(principal)) {
        // if no login identity is available, fail
        UserGroupInformation loginUser = null;
        try {
          loginUser = getLoginUser();
        } catch (IOException e) {
          throw new SliderException(EXIT_UNAUTHORIZED, e,
                                    "No principal configured for the application and "
                                    + "exception raised during retrieval of login user. "
                                    + "Unable to proceed with application "
                                    + "initialization.  Please ensure a value "
                                    + "for %s exists in the application "
                                    + "configuration or the login issue is addressed",
                                    SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL);
        }
        if (loginUser == null) {
          throw new SliderException(EXIT_UNAUTHORIZED,
                                    "No principal configured for the application "
                                    + "and no login user found. "
                                    + "Unable to proceed with application "
                                    + "initialization.  Please ensure a value "
                                    + "for %s exists in the application "
                                    + "configuration or the login issue is addressed",
                                    SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL);
        }
      }
      // ensure that either local or distributed keytab mechanism is enabled,
      // but not both
      String keytabFullPath = instanceDefinition.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM)
          .get(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
      String keytabName = instanceDefinition.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM)
          .get(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
      if (SliderUtils.isSet(keytabFullPath) && SliderUtils.isSet(keytabName)) {
        throw new SliderException(EXIT_UNAUTHORIZED,
                                  "Both a keytab on the cluster host (%s) and a"
                                  + " keytab to be retrieved from HDFS (%s) are"
                                  + " specified.  Please configure only one keytab"
                                  + " retrieval mechanism.",
                                  SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH,
                                  SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);

      }
    }
  }

  protected UserGroupInformation getLoginUser() throws IOException {
    return UserGroupInformation.getLoginUser();
  }

  public boolean isSecurityEnabled () {
    return SliderUtils.isHadoopClusterSecure(configuration);
  }

  public String getPrincipal () throws IOException {
    String principal = instanceDefinition.getAppConfOperations()
        .getComponent(SliderKeys.COMPONENT_AM).get(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL);
    if (SliderUtils.isUnset(principal)) {
      principal = UserGroupInformation.getLoginUser().getShortUserName();
      log.info("No principal set in the slider configuration.  Will use AM login"
               + " identity {} to attempt keytab-based login", principal);
    }

    return principal;
  }

  public boolean isKeytabProvided() {
    boolean keytabProvided = instanceDefinition.getAppConfOperations()
                    .getComponent(SliderKeys.COMPONENT_AM)
                    .get(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH) != null ||
                instanceDefinition.getAppConfOperations()
                    .getComponent(SliderKeys.COMPONENT_AM).
                    get(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME) != null;
    return keytabProvided;

  }

  public File getKeytabFile(AggregateConf instanceDefinition)
      throws SliderException, IOException {
    String keytabFullPath = instanceDefinition.getAppConfOperations()
        .getComponent(SliderKeys.COMPONENT_AM)
        .get(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
    File localKeytabFile;
    if (SliderUtils.isUnset(keytabFullPath)) {
      // get the keytab
      String keytabName = instanceDefinition.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM).
              get(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
      log.info("No host keytab file path specified. Will attempt to retrieve"
               + " keytab file {} as a local resource for the container",
               keytabName);
      // download keytab to local, protected directory
      localKeytabFile = new File(SliderKeys.KEYTAB_DIR, keytabName);
    } else {
      log.info("Using host keytab file {} for login", keytabFullPath);
      localKeytabFile = new File(keytabFullPath);
    }
    return localKeytabFile;
  }

}
