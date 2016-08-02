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

package org.apache.slider.common.tools;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Locale;
import java.util.Properties;

/**
 * Extract the version properties, which will look something like
 * <pre>
 * application.name=${pom.name}
 * application.version=${pom.version}
 * application.build=${buildNumber}
 * application.build.java.version=${java.version}
 * application.build.info=${pom.name}-${pom.version} Built against ${buildNumber} on ${java.version} by ${user.name}
 * </pre>
 * 
 * the <code>mvn process-resources</code> target will expand the properties
 * and add the resources to target/classes, which will then look something like
 * <pre>
 *   application.name=Slider Core
 *   application.version=0.7.1-SNAPSHOT
 *   application.build=1dd69
 *   application.build.java.version=1.7.0_45
 *   application.build.user=stevel
 *   application.build.info=Slider Core-0.7.1-SNAPSHOT Built against 1dd69 on 1.7.0_45 by stevel
 * </pre>
 * 
 * Note: the values will change and more properties added.
 */
public class SliderVersionInfo {
  private static final Logger log = LoggerFactory.getLogger(SliderVersionInfo.class);

  /**
   * Name of the resource containing the filled-in-at-runtime props
   */
  public static final String VERSION_RESOURCE =
      "org/apache/slider/providers/dynamic/application.properties";

  public static final String APP_NAME = "application.name";
  public static final String APP_VERSION = "application.version";
  public static final String APP_BUILD = "application.build";
  public static final String APP_BUILD_JAVA_VERSION = "application.build.java.version";
  public static final String APP_BUILD_USER = "application.build.user";
  public static final String APP_BUILD_INFO = "application.build.info";
  public static final String HADOOP_BUILD_INFO = "hadoop.build.info";
  public static final String HADOOP_DEPLOYED_INFO = "hadoop.deployed.info";


  public static Properties loadVersionProperties()  {
    Properties props = new Properties();
    URL resURL = SliderVersionInfo.class.getClassLoader()
                                   .getResource(VERSION_RESOURCE);
    assert resURL != null : "Null resource " + VERSION_RESOURCE;

    try {
      InputStream inStream = resURL.openStream();
      assert inStream != null : "Null input stream from " + VERSION_RESOURCE;
      props.load(inStream);
    } catch (IOException e) {
      log.warn("IOE loading " + VERSION_RESOURCE, e);
    }
    return props;
  }

  /**
   * Load the version info and print it
   * @param logger logger
   */
  public static void loadAndPrintVersionInfo(Logger logger) {
    Properties props = loadVersionProperties();
    logger.info(props.getProperty(APP_BUILD_INFO));
    logger.info("Compiled against Hadoop {}",
                props.getProperty(HADOOP_BUILD_INFO));
    logger.info(getHadoopVersionString());
  }
  
  public static String getHadoopVersionString() {
    return String.format(Locale.ENGLISH,
        "Hadoop runtime version %s with source checksum %s and build date %s",
        VersionInfo.getBranch(),
        VersionInfo.getSrcChecksum(),
        VersionInfo.getDate());
  }
}
