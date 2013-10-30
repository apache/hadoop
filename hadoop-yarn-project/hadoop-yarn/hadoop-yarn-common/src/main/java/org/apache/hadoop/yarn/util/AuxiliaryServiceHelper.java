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

package org.apache.hadoop.yarn.util;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;


public class AuxiliaryServiceHelper {

  public final static String NM_AUX_SERVICE = "NM_AUX_SERVICE_";

  public static ByteBuffer getServiceDataFromEnv(String serviceName,
      Map<String, String> env) {
    String meta = env.get(getPrefixServiceName(serviceName));
    if (null == meta) {
      return null;
    }
    byte[] metaData = Base64.decodeBase64(meta);
    return ByteBuffer.wrap(metaData);
  }

  public static void setServiceDataIntoEnv(String serviceName,
      ByteBuffer metaData, Map<String, String> env) {
    byte[] byteData = metaData.array();
    env.put(getPrefixServiceName(serviceName),
        Base64.encodeBase64String(byteData));
  }

  private static String getPrefixServiceName(String serviceName) {
    return NM_AUX_SERVICE + serviceName;
  }
}
