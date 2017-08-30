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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;

/**
 * Factory methods for instantiating a timeline entity reader.
 */
public final class TimelineEntityReaderFactory {
  private TimelineEntityReaderFactory() {
  }

  /**
   * Creates a timeline entity reader instance for reading a single entity with
   * the specified input.
   *
   * @param context Reader context which defines the scope in which query has to
   *     be made.
   * @param dataToRetrieve Data to retrieve for each entity.
   * @return An implementation of <cite>TimelineEntityReader</cite> object
   *     depending on entity type.
   */
  public static TimelineEntityReader createSingleEntityReader(
      TimelineReaderContext context, TimelineDataToRetrieve dataToRetrieve) {
    // currently the types that are handled separate from the generic entity
    // table are application, flow run, and flow activity entities
    if (TimelineEntityType.YARN_APPLICATION.matches(context.getEntityType())) {
      return new ApplicationEntityReader(context, dataToRetrieve);
    } else if (TimelineEntityType.
        YARN_FLOW_RUN.matches(context.getEntityType())) {
      return new FlowRunEntityReader(context, dataToRetrieve);
    } else if (TimelineEntityType.
        YARN_FLOW_ACTIVITY.matches(context.getEntityType())) {
      return new FlowActivityEntityReader(context, dataToRetrieve);
    } else {
      // assume we're dealing with a generic entity read
      return new GenericEntityReader(context, dataToRetrieve);
    }
  }

  /**
   * Creates a timeline entity reader instance for reading set of entities with
   * the specified input and predicates.
   *
   * @param context Reader context which defines the scope in which query has to
   *     be made.
   * @param filters Filters which limit the entities returned.
   * @param dataToRetrieve Data to retrieve for each entity.
   * @return An implementation of <cite>TimelineEntityReader</cite> object
   *     depending on entity type.
   */
  public static TimelineEntityReader createMultipleEntitiesReader(
      TimelineReaderContext context, TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve) {
    // currently the types that are handled separate from the generic entity
    // table are application, flow run, and flow activity entities
    if (TimelineEntityType.YARN_APPLICATION.matches(context.getEntityType())) {
      return new ApplicationEntityReader(context, filters, dataToRetrieve);
    } else if (TimelineEntityType.
        YARN_FLOW_ACTIVITY.matches(context.getEntityType())) {
      return new FlowActivityEntityReader(context, filters, dataToRetrieve);
    } else if (TimelineEntityType.
        YARN_FLOW_RUN.matches(context.getEntityType())) {
      return new FlowRunEntityReader(context, filters, dataToRetrieve);
    } else {
      if (context.getDoAsUser() != null) {
        return new SubApplicationEntityReader(context, filters, dataToRetrieve);
      }
      // assume we're dealing with a generic entity read
      return new GenericEntityReader(context, filters, dataToRetrieve);
    }
  }

  /**
   * Creates a timeline entity type reader that will read all available entity
   * types within the specified context.
   *
   * @param context Reader context which defines the scope in which query has to
   *                be made. Limited to application level only.
   * @return an <cite>EntityTypeReader</cite> object
   */
  public static EntityTypeReader createEntityTypeReader(
      TimelineReaderContext context) {
    return new EntityTypeReader(context);
  }
}
