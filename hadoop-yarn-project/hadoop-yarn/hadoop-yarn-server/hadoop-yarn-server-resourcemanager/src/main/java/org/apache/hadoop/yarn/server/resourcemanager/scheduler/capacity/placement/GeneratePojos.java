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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import org.jsonschema2pojo.DefaultGenerationConfig;
import org.jsonschema2pojo.GenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;
import org.jsonschema2pojo.SchemaGenerator;
import org.jsonschema2pojo.SchemaMapper;
import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.rules.RuleFactory;

import com.sun.codemodel.JCodeModel;

/**
 * Helper class to re-generate java POJOs based on the JSON schema.
 */
public final class GeneratePojos {
  @SuppressWarnings("checkstyle:linelength")
  private static final String TARGET_PACKAGE =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema";

  private GeneratePojos() {
    // no instances
  }

  public static void main(String[] args) throws IOException {
    JCodeModel codeModel = new JCodeModel();
    URL schemaURL = Paths.get(
        "src/main/json_schema/MappingRulesDescription.json").toUri().toURL();

    GenerationConfig config = new DefaultGenerationConfig() {
      @Override
      public boolean isGenerateBuilders() {
        return false;
      }

      @Override
      public boolean isUsePrimitives() {
          return true;
      }
    };

    SchemaMapper mapper =
        new SchemaMapper(
            new RuleFactory(config,
                new Jackson2Annotator(config),
                new SchemaStore()),
            new SchemaGenerator());

    mapper.generate(codeModel, "ignore", TARGET_PACKAGE, schemaURL);

    codeModel.build(new File("src/main/java"));
  }
}
