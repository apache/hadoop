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
package org.apache.hadoop.fs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

/** 
 * This class is thin layer to manage the FS related keys in
 * a configuration object.
 * It provides convenience static method to set and get the keys from a 
 * a configuration.
 *
 */

final class FsConfig {
  private FsConfig() {}
  
  // Configuration keys  and default values in the config file
  // TBD note we should deprecate the keys constants elsewhere
  
  
  // The Keys
  static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
  static final String FS_HOME_DIR_ROOT_KEY = "fs.homeDir";
  static final String FS_REPLICATION_FACTOR_KEY = "dfs.replication";
  static final String FS_BLOCK_SIZE_KEY = "dfs.block.size";
  static final String IO_BUFFER_SIZE_KEY ="io.file.buffer.size";


  // The default values
  // Default values of SERVER_DEFAULT(-1) implies use the ones from
  // the target file system where files are created.
  static final String FS_DEFAULT_NAME = "file:///";
  static final String FS_HOME_DIR_ROOT = "/user"; // relative to FS_DEFAULT
  static final short FS_DEFAULT_REPLICATION_FACTOR = 3;
  static final long FS_DEFAULT_BLOCK_SIZE = 32 * 1024 * 1024;
  static final int IO_BUFFER_SIZE =4096;
  
  
  
  public static String getDefaultFsURI(final Configuration conf) {
    return conf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME);
  }
  
  public static String getHomeDir(final Configuration conf) {
    return conf.get(FS_HOME_DIR_ROOT_KEY, FS_HOME_DIR_ROOT);
  }
  
  public static short getDefaultReplicationFactor(final Configuration conf) {
    return (short) 
        conf.getInt(FS_REPLICATION_FACTOR_KEY, FS_DEFAULT_REPLICATION_FACTOR);
  }
  
  public static long getDefaultBlockSize(final Configuration conf) {
    return conf.getLong(FS_BLOCK_SIZE_KEY, FS_DEFAULT_BLOCK_SIZE);
  }

  
  public static int getDefaultIOBuffersize(final Configuration conf) {
    return conf.getInt(IO_BUFFER_SIZE_KEY, IO_BUFFER_SIZE);
  }
  
  public static Class<?> getImplClass(URI uri, Configuration conf) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("No scheme");
    }
    return conf.getClass("fs." + uri.getScheme() + ".impl", null);
  }

  
  /**
   * The Setters: see the note on the javdoc for the class above.
   */

  public static void setDefaultFS(final Configuration conf, String uri) {
    conf.set(FS_DEFAULT_NAME_KEY, uri);
  }
  
  public static void setHomeDir(final Configuration conf, String path) {
    conf.set(FS_HOME_DIR_ROOT_KEY, path);
  }
  
  public static void setDefaultReplicationFactor(final Configuration conf,
    short rf) {
    conf.setInt(FS_REPLICATION_FACTOR_KEY, rf);
  }
  
  public static void setDefaultBlockSize(final Configuration conf, long bs) {
    conf.setLong(FS_BLOCK_SIZE_KEY, bs);
  }
  
  public static void setDefaultIOBuffersize(final Configuration conf, int bs) {
    conf.setInt(IO_BUFFER_SIZE_KEY, bs);
  }
}
