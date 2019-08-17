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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This creates the timeline schema for storing application timeline
 * information. Each backend has to implement the {@link SchemaCreator} for
 * creating the schema in its backend and should be configured in yarn-site.xml.
 */
public class TimelineSchemaCreator extends Configured implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineSchemaCreator.class);

  public static void main(String[] args) {
    try {
      int status = ToolRunner.run(new YarnConfiguration(),
          new TimelineSchemaCreator(), args);
      System.exit(status);
    } catch (Exception e) {
      LOG.error("Error while creating Timeline Schema : ", e);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    return createTimelineSchema(args, conf);
  }

  @VisibleForTesting
  int createTimelineSchema(String[] args, Configuration conf) throws Exception {
    String schemaCreatorClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS);
    LOG.info("Using {} for creating Timeline Service Schema ",
        schemaCreatorClassName);
    try {
      Class<?> schemaCreatorClass = Class.forName(schemaCreatorClassName);
      if (SchemaCreator.class.isAssignableFrom(schemaCreatorClass)) {
        SchemaCreator schemaCreator = (SchemaCreator) ReflectionUtils
            .newInstance(schemaCreatorClass, conf);
        schemaCreator.createTimelineSchema(args);
        return 0;
      } else {
        throw new YarnRuntimeException("Class: " + schemaCreatorClassName
            + " not instance of " + SchemaCreator.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineReader: "
          + schemaCreatorClassName, e);
    }
  }
}