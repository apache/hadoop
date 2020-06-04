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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;

/**
 * FPGA device discovery strategy which invokes an external script.
 * The script must return a single line in given format.
 *
 * See DeviceSpecParser for details.
 */
public class ScriptBasedFPGADiscoveryStrategy
    implements FPGADiscoveryStrategy {

  private final Function<String, Optional<String>> scriptRunner;
  private final String discoveryScript;
  private final String type;

  public ScriptBasedFPGADiscoveryStrategy(
      String fpgaType,
      Function<String, Optional<String>> scriptRunner,
      String propValue) {
    this.scriptRunner = scriptRunner;
    this.discoveryScript = propValue;
    this.type = fpgaType;
  }

  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    Optional<String> scriptOutput =
        scriptRunner.apply(discoveryScript);
    if (scriptOutput.isPresent()) {
      List<FpgaDevice> list =
          DeviceSpecParser.getDevicesFromString(type, scriptOutput.get());
      if (list.isEmpty()) {
        throw new ResourceHandlerException("No FPGA devices were specified");
      }
      return list;
    } else {
      throw new ResourceHandlerException("Unable to run external script");
    }
  }
}