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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLStreamHandler;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * URLStream handler relying on FileSystem and on a given Configuration to
 * handle URL protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FsUrlStreamHandler extends URLStreamHandler {

  private Configuration conf;

  FsUrlStreamHandler(Configuration conf) {
    this.conf = conf;
  }

  FsUrlStreamHandler() {
    this.conf = new Configuration();
  }

  @Override
  protected void parseURL(URL u, String spec, int start, int limit) {
    if (spec.startsWith("file:")) {
      super.parseURL(u, spec.replace(File.separatorChar, '/'), start, limit);
    } else {
      super.parseURL(u, spec, start, limit);
    }
  }

  @Override
  protected FsUrlConnection openConnection(URL url) throws IOException {
    return new FsUrlConnection(conf, url);
  }

}
