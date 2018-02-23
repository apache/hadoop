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

package org.apache.hadoop.hdfs.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;

/**
 * Writer support for JSON-based datanode configuration, an alternative format
 * to the exclude/include files configuration.
 * The JSON file format defines the array of elements where each element
 * in the array describes the properties of a datanode. The properties of
 * a datanode is defined by {@link DatanodeAdminProperties}. For example,
 *
 * [
 *   {"hostName": "host1"},
 *   {"hostName": "host2", "port": 50, "upgradeDomain": "ud0"},
 *   {"hostName": "host3", "port": 0, "adminState": "DECOMMISSIONED"}
 * ]
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public final class CombinedHostsFileWriter {
  private CombinedHostsFileWriter() {
  }

  /**
   * Serialize a set of DatanodeAdminProperties to a json file.
   * @param hostsFile the json file name.
   * @param allDNs the set of DatanodeAdminProperties
   * @throws IOException
   */
  public static void writeFile(final String hostsFile,
      final Set<DatanodeAdminProperties> allDNs) throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();

    try (Writer output =
       new OutputStreamWriter(new FileOutputStream(hostsFile), "UTF-8")) {
      objectMapper.writeValue(output, allDNs);
    }
  }
}
