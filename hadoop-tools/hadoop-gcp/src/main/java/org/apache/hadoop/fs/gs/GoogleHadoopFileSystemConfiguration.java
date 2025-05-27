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

package org.apache.hadoop.fs.gs;

import static java.lang.Math.toIntExact;

import org.apache.hadoop.conf.Configuration;

/**
 * This class provides a configuration for the {@link GoogleHadoopFileSystem} implementations.
 */
class GoogleHadoopFileSystemConfiguration {
  /**
   * Configuration key for default block size of a file.
   *
   * <p>Note that this is the size that is reported to Hadoop FS clients. It does not modify the
   * actual block size of an underlying GCS object, because GCS JSON API does not allow modifying or
   * querying the value. Modifying this value allows one to control how many mappers are used to
   * process a given file.
   */
  public static final HadoopConfigurationProperty<Long> BLOCK_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.block.size", 64 * 1024 * 1024L);

  /**
   * Configuration key for GCS project ID. Default value: none
   */
  public static final HadoopConfigurationProperty<String> GCS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.project.id");

  /**
   * Configuration key for initial working directory of a GHFS instance. Default value: '/'
   */
  public static final HadoopConfigurationProperty<String> GCS_WORKING_DIRECTORY =
      new HadoopConfigurationProperty<>("fs.gs.working.dir", "/");

  /**
   * Configuration key for setting write buffer size.
   */
  public static final HadoopConfigurationProperty<Long> GCS_OUTPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.buffer.size", 8L * 1024 * 1024);

  private final String workingDirectory;

  public int getOutStreamBufferSize() {
    return outStreamBufferSize;
  }

  private final int outStreamBufferSize;

  GoogleHadoopFileSystemConfiguration(Configuration config) {
    this.workingDirectory = GCS_WORKING_DIRECTORY.get(config, config::get);
    this.outStreamBufferSize =
        toIntExact(GCS_OUTPUT_STREAM_BUFFER_SIZE.get(config, config::getLongBytes));
  }

  public String getWorkingDirectory() {
    return this.workingDirectory;
  }
}
