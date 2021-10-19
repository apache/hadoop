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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * An interface for loading mount-table configuration. This class can have more
 * APIs like refreshing mount tables automatically etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MountTableConfigLoader {

  /**
   * Loads the mount-table configuration into given configuration.
   *
   * @param mountTableConfigPath - Path of the mount table. It can be a file or
   *          a directory in the case of multiple versions of mount-table
   *          files(Recommended option).
   * @param conf - Configuration object to add mount table.
   */
  void load(String mountTableConfigPath, Configuration conf)
      throws IOException;
}
