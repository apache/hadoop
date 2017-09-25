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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader support for JSON-based datanode configuration, an alternative format
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
public final class CombinedHostsFileReader {

  public static final Logger LOG =
      LoggerFactory.getLogger(CombinedHostsFileReader.class);

  private CombinedHostsFileReader() {
  }

  /**
   * Deserialize a set of DatanodeAdminProperties from a json file.
   * @param hostsFile the input json file to read from
   * @return the set of DatanodeAdminProperties
   * @throws IOException
   */
  public static DatanodeAdminProperties[]
      readFile(final String hostsFile) throws IOException {
    DatanodeAdminProperties[] allDNs = new DatanodeAdminProperties[0];
    ObjectMapper objectMapper = new ObjectMapper();
    boolean tryOldFormat = false;
    try (Reader input =
        new InputStreamReader(new FileInputStream(hostsFile), "UTF-8")) {
      allDNs = objectMapper.readValue(input, DatanodeAdminProperties[].class);
    } catch (JsonMappingException jme) {
      // The old format doesn't have json top-level token to enclose the array.
      // For backward compatibility, try parsing the old format.
      tryOldFormat = true;
      LOG.warn("{} has invalid JSON format." +
          "Try the old format without top-level token defined.", hostsFile);
    }

    if (tryOldFormat) {
      ObjectReader objectReader =
          objectMapper.readerFor(DatanodeAdminProperties.class);
      JsonFactory jsonFactory = new JsonFactory();
      List<DatanodeAdminProperties> all = new ArrayList<>();
      try (Reader input =
          new InputStreamReader(new FileInputStream(hostsFile), "UTF-8")) {
        Iterator<DatanodeAdminProperties> iterator =
            objectReader.readValues(jsonFactory.createParser(input));
        while (iterator.hasNext()) {
          DatanodeAdminProperties properties = iterator.next();
          all.add(properties);
        }
      }
      allDNs = all.toArray(new DatanodeAdminProperties[all.size()]);
    }
    return allDNs;
  }
}
