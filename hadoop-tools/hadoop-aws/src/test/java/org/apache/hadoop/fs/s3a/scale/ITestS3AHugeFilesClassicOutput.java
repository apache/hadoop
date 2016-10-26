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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

/**
 * Use classic output for writing things; tweaks the configuration to do
 * this after it has been set up in the superclass.
 * The generator test has been copied and re
 */
public class ITestS3AHugeFilesClassicOutput extends AbstractSTestS3AHugeFiles {

  @Override
  protected Configuration createScaleConfiguration() {
    final Configuration conf = super.createScaleConfiguration();
    conf.setBoolean(Constants.FAST_UPLOAD, false);
    return conf;
  }

  protected String getBlockOutputBufferName() {
    return "classic";
  }
}
