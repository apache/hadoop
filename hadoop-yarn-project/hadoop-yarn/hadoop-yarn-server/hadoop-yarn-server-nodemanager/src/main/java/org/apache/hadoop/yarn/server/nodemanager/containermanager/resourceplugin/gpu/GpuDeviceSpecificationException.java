/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This exception is to be thrown when allowed GPU device specification
 * is empty or invalid.
 */
public final class GpuDeviceSpecificationException extends YarnException {
  private static final String VALID_FORMAT_MESSAGE = "The valid format " +
      "should be: index:minor_number";

  private GpuDeviceSpecificationException(String message) {
    super(message);
  }

  private GpuDeviceSpecificationException(String message, Exception cause) {
    super(message, cause);
  }

  public static GpuDeviceSpecificationException createWithEmptyValueSpecified() {
    return new GpuDeviceSpecificationException(
        YarnConfiguration.NM_GPU_ALLOWED_DEVICES +
        " is set to an empty value! Please specify " +
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES +
        " to enable auto-discovery or " +
        "please enter the GPU device IDs manually! " +
            VALID_FORMAT_MESSAGE);
  }

  public static GpuDeviceSpecificationException createWithWrongValueSpecified(
      String device, String configValue, Exception cause) {
    final String message = createIllegalFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message, cause);
  }

  public static GpuDeviceSpecificationException createWithWrongValueSpecified(
      String device, String configValue) {
    final String message = createIllegalFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message);
  }

  public static GpuDeviceSpecificationException createWithDuplicateValueSpecified(
      String device, String configValue) {
    final String message = createDuplicateFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message);
  }

  private static String createIllegalFormatMessage(String device,
      String configValue) {
    return String.format("Illegal format of individual GPU device: %s, " +
            "the whole config value was: '%s'! " + VALID_FORMAT_MESSAGE,
        device, configValue);
  }

  private static String createDuplicateFormatMessage(String device,
      String configValue) {
    return String.format("GPU device %s" +
            " has a duplicate definition! " +
            "Please double-check the configuration " +
            YarnConfiguration.NM_GPU_ALLOWED_DEVICES +
            "! Current value of the configuration is: %s",
        device, configValue);
  }
}