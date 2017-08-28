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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Extends Core Filesystem with operations to manipulate ClusterDescription
 * persistent state
 */
public class SliderFileSystem extends CoreFileSystem {

  Path appDir = null;

  public SliderFileSystem(FileSystem fileSystem,
      Configuration configuration) {
    super(fileSystem, configuration);
  }

  public SliderFileSystem(Configuration configuration) throws IOException {
    super(configuration);
  }

  public void setAppDir(Path appDir) {
    this.appDir = appDir;
  }

  public Path getAppDir() {
    return this.appDir;
  }
}
