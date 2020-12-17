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
package org.apache.hadoop.fs.viewfs;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Utilities for config variables of the viewFs See {@link ViewFs}
 */
public class ConfigUtil {
  /**
   * Get the config variable prefix for the specified mount table
   * @param mountTableName - the name of the mount table
   * @return the config variable prefix for the specified mount table
   */
  public static String getConfigViewFsPrefix(final String mountTableName) {
    return Constants.CONFIG_VIEWFS_PREFIX + "." + mountTableName;
  }
  
  /**
   * Get the config variable prefix for the default mount table
   * @return the config variable prefix for the default mount table
   */
  public static String getConfigViewFsPrefix() {
    return 
      getConfigViewFsPrefix(Constants.CONFIG_VIEWFS_PREFIX_DEFAULT_MOUNT_TABLE);
  }
  
  /**
   * Add a link to the config for the specified mount table
   * @param conf - add the link to this conf
   * @param mountTableName
   * @param src - the src path name
   * @param target - the target URI link
   */
  public static void addLink(Configuration conf, final String mountTableName, 
      final String src, final URI target) {
    conf.set(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_LINK + "." + src, target.toString());  
  }
  
  /**
   * Add a link to the config for the default mount table
   * @param conf - add the link to this conf
   * @param src - the src path name
   * @param target - the target URI link
   */
  public static void addLink(final Configuration conf, final String src,
      final URI target) {
    addLink(conf, getDefaultMountTableName(conf), src, target);
  }

  /**
   * Add a LinkMergeSlash to the config for the specified mount table.
   * @param conf
   * @param mountTableName
   * @param target
   */
  public static void addLinkMergeSlash(Configuration conf,
      final String mountTableName, final URI target) {
    conf.set(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH, target.toString());
  }

  /**
   * Add a LinkMergeSlash to the config for the default mount table.
   * @param conf
   * @param target
   */
  public static void addLinkMergeSlash(Configuration conf, final URI target) {
    addLinkMergeSlash(conf, getDefaultMountTableName(conf), target);
  }

  /**
   * Add a LinkFallback to the config for the specified mount table.
   * @param conf
   * @param mountTableName
   * @param target
   */
  public static void addLinkFallback(Configuration conf,
      final String mountTableName, final URI target) {
    conf.set(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_LINK_FALLBACK, target.toString());
  }

  /**
   * Add a LinkFallback to the config for the default mount table.
   * @param conf
   * @param target
   */
  public static void addLinkFallback(Configuration conf, final URI target) {
    addLinkFallback(conf, getDefaultMountTableName(conf), target);
  }

  /**
   * Add a LinkMerge to the config for the specified mount table.
   * @param conf
   * @param mountTableName
   * @param targets
   */
  public static void addLinkMerge(Configuration conf,
      final String mountTableName, final URI[] targets) {
    conf.set(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_LINK_MERGE, Arrays.toString(targets));
  }

  /**
   * Add a LinkMerge to the config for the default mount table.
   * @param conf
   * @param targets
   */
  public static void addLinkMerge(Configuration conf, final URI[] targets) {
    addLinkMerge(conf, getDefaultMountTableName(conf), targets);
  }

  /**
   * Add nfly link to configuration for the given mount table.
   */
  public static void addLinkNfly(Configuration conf, String mountTableName,
      String src, String settings, final String targets) {
    conf.set(
        getConfigViewFsPrefix(mountTableName) + "."
            + Constants.CONFIG_VIEWFS_LINK_NFLY + "." + settings + "." + src,
        targets);
  }

  /**
   *
   * @param conf
   * @param mountTableName
   * @param src
   * @param settings
   * @param targets
   */
  public static void addLinkNfly(Configuration conf, String mountTableName,
      String src, String settings, final URI ... targets) {

    settings = settings == null
        ? "minReplication=2,repairOnRead=true"
        : settings;
    addLinkNfly(conf, mountTableName, src, settings,
        StringUtils.uriToString(targets));
  }

  public static void addLinkNfly(final Configuration conf, final String src,
      final URI ... targets) {
    addLinkNfly(conf, getDefaultMountTableName(conf), src, null, targets);
  }

  /**
   * Add a LinkRegex to the config for the specified mount table.
   * @param conf - get mountable config from this conf
   * @param mountTableName - the mountable name of the regex config item
   * @param srcRegex - the src path regex expression that applies to this config
   * @param targetStr - the string of target path
   * @param interceptorSettings - the serialized interceptor string to be
   *                            applied while resolving the mapping
   */
  public static void addLinkRegex(
      Configuration conf, final String mountTableName, final String srcRegex,
      final String targetStr, final String interceptorSettings) {
    String prefix = getConfigViewFsPrefix(mountTableName) + "."
        + Constants.CONFIG_VIEWFS_LINK_REGEX + ".";
    if ((interceptorSettings != null) && (!interceptorSettings.isEmpty())) {
      prefix = prefix + interceptorSettings
          + RegexMountPoint.SETTING_SRCREGEX_SEP;
    }
    String key = prefix + srcRegex;
    conf.set(key, targetStr);
  }

  /**
   * Add config variable for homedir for default mount table
   * @param conf - add to this conf
   * @param homedir - the home dir path starting with slash
   */
  public static void setHomeDirConf(final Configuration conf,
      final String homedir) {
    setHomeDirConf(conf, getDefaultMountTableName(conf), homedir);
  }
  
  /**
   * Add config variable for homedir the specified mount table
   * @param conf - add to this conf
   * @param homedir - the home dir path starting with slash
   */
  public static void setHomeDirConf(final Configuration conf,
              final String mountTableName, final String homedir) {
    if (!homedir.startsWith("/")) {
      throw new IllegalArgumentException("Home dir should start with /:"
          + homedir);
    }
    conf.set(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_HOMEDIR, homedir);
  }
  
  /**
   * Get the value of the home dir conf value for default mount table
   * @param conf - from this conf
   * @return home dir value, null if variable is not in conf
   */
  public static String getHomeDirValue(final Configuration conf) {
    return getHomeDirValue(conf, getDefaultMountTableName(conf));
  }
  
  /**
   * Get the value of the home dir conf value for specified mount table
   * @param conf - from this conf
   * @param mountTableName - the mount table
   * @return home dir value, null if variable is not in conf
   */
  public static String getHomeDirValue(final Configuration conf, 
      final String mountTableName) {
    return conf.get(getConfigViewFsPrefix(mountTableName) + "." +
        Constants.CONFIG_VIEWFS_HOMEDIR);
  }

  /**
   * Get the name of the default mount table to use. If
   * {@link Constants#CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE_NAME_KEY} is specified,
   * it's value is returned. Otherwise,
   * {@link Constants#CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE} is returned.
   *
   * @param conf Configuration to use.
   * @return the name of the default mount table to use.
   */
  public static String getDefaultMountTableName(final Configuration conf) {
    return conf.get(Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE_NAME_KEY,
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE);
  }
}
