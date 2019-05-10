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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.service.api.records.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;

/**
 * This class is merely responsible for creating Json representation of
 * {@link Service} instances.
 */
public final class ServiceSpecFileGenerator {
  private ServiceSpecFileGenerator() {
    throw new UnsupportedOperationException("This class should not be " +
        "instantiated!");
  }

  public static String generateJson(Service service) throws IOException {
    File serviceSpecFile = File.createTempFile(service.getName(), ".json");
    String buffer = jsonSerDeser.toJson(service);
    Writer w = new OutputStreamWriter(new FileOutputStream(serviceSpecFile),
        StandardCharsets.UTF_8);
    try (PrintWriter pw = new PrintWriter(w)) {
      pw.append(buffer);
    }
    return serviceSpecFile.getAbsolutePath();
  }
}
