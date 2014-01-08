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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;


public class Log4jPropertyHelper {

  public static void updateLog4jConfiguration(Class<?> targetClass,
      String log4jPath) throws Exception {
    Properties customProperties = new Properties();
    FileInputStream fs = null;
    InputStream is = null;
    try {
      fs = new FileInputStream(log4jPath);
      is = targetClass.getResourceAsStream("/log4j.properties");
      customProperties.load(fs);
      Properties originalProperties = new Properties();
      originalProperties.load(is);
      for (Entry<Object, Object> entry : customProperties.entrySet()) {
        originalProperties.setProperty(entry.getKey().toString(), entry
            .getValue().toString());
      }
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    }finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(fs);
    }
  }
}
