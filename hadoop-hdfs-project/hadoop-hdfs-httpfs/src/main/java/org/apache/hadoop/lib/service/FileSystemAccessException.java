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

package org.apache.hadoop.lib.service;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.lang.XException;

@InterfaceAudience.Private
public class FileSystemAccessException extends XException {

  public enum ERROR implements XException.ERROR {
    H01("Service property [{0}] not defined"),
    H02("Kerberos initialization failed, {0}"),
    H03("FileSystemExecutor error, {0}"),
    H04("Invalid configuration, it has not be created by the FileSystemAccessService"),
    H05("[{0}] validation failed, {1}"),
    H06("Property [{0}] not defined in configuration object"),
    H07("[{0}] not healthy, {1}"),
    H08("{0}"),
    H09("Invalid FileSystemAccess security mode [{0}]"),
    H10("Hadoop config directory not found [{0}]"),
    H11("Could not load Hadoop config files, {0}");

    private String template;

    ERROR(String template) {
      this.template = template;
    }

    @Override
    public String getTemplate() {
      return template;
    }
  }

  public FileSystemAccessException(ERROR error, Object... params) {
    super(error, params);
  }

}
