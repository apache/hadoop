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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utility class to load KMS configuration files.
 */
@InterfaceAudience.Private
public class KMSConfiguration {

  public static final String KMS_CONFIG_DIR = "kms.config.dir";
  public static final String KMS_SITE_XML = "kms-site.xml";
  public static final String KMS_ACLS_XML = "kms-acls.xml";

  public static final String CONFIG_PREFIX = "hadoop.kms.";

  public static final String KEY_ACL_PREFIX = "key.acl.";
  public static final String DEFAULT_KEY_ACL_PREFIX = "default.key.acl.";
  public static final String WHITELIST_KEY_ACL_PREFIX = "whitelist.key.acl.";

  // Property to set the backing KeyProvider
  public static final String KEY_PROVIDER_URI = CONFIG_PREFIX +
      "key.provider.uri";

  // Property to Enable/Disable Caching
  public static final String KEY_CACHE_ENABLE = CONFIG_PREFIX +
      "cache.enable";
  // Timeout for the Key and Metadata Cache
  public static final String KEY_CACHE_TIMEOUT_KEY = CONFIG_PREFIX +
      "cache.timeout.ms";
  // TImeout for the Current Key cache
  public static final String CURR_KEY_CACHE_TIMEOUT_KEY = CONFIG_PREFIX +
      "current.key.cache.timeout.ms";
  // Delay for Audit logs that need aggregation
  public static final String KMS_AUDIT_AGGREGATION_WINDOW = CONFIG_PREFIX +
      "audit.aggregation.window.ms";
  
  public static final boolean KEY_CACHE_ENABLE_DEFAULT = true;
  // 10 mins
  public static final long KEY_CACHE_TIMEOUT_DEFAULT = 10 * 60 * 1000;
  // 30 secs
  public static final long CURR_KEY_CACHE_TIMEOUT_DEFAULT = 30 * 1000;
  // 10 secs
  public static final long KMS_AUDIT_AGGREGATION_WINDOW_DEFAULT = 10000;

  // Property to Enable/Disable per Key authorization
  public static final String KEY_AUTHORIZATION_ENABLE = CONFIG_PREFIX +
      "key.authorization.enable"; 

  public static final boolean KEY_AUTHORIZATION_ENABLE_DEFAULT = true;

  static Configuration getConfiguration(boolean loadHadoopDefaults,
      String ... resources) {
    Configuration conf = new Configuration(loadHadoopDefaults);
    String confDir = System.getProperty(KMS_CONFIG_DIR);
    if (confDir != null) {
      try {
        Path confPath = new Path(confDir);
        if (!confPath.isUriPathAbsolute()) {
          throw new RuntimeException("System property '" + KMS_CONFIG_DIR +
              "' must be an absolute path: " + confDir);
        }
        for (String resource : resources) {
          conf.addResource(new URL("file://" + new Path(confDir, resource).toUri()));
        }
      } catch (MalformedURLException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      for (String resource : resources) {
        conf.addResource(resource);
      }
    }
    return conf;
  }

  public static Configuration getKMSConf() {
    return getConfiguration(true, "core-site.xml", KMS_SITE_XML);
  }

  public static Configuration getACLsConf() {
    return getConfiguration(false, KMS_ACLS_XML);
  }

  public static boolean isACLsFileNewer(long time) {
    boolean newer = false;
    String confDir = System.getProperty(KMS_CONFIG_DIR);
    if (confDir != null) {
      Path confPath = new Path(confDir);
      if (!confPath.isUriPathAbsolute()) {
        throw new RuntimeException("System property '" + KMS_CONFIG_DIR +
            "' must be an absolute path: " + confDir);
      }
      File f = new File(confDir, KMS_ACLS_XML);
      // at least 100ms newer than time, we do this to ensure the file
      // has been properly closed/flushed
      newer = f.lastModified() - time > 100;
    }
    return newer;
  }
}
