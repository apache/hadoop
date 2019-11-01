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

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Stub based implementation for TimelineReader. This implementation will
 * not provide a complete implementation of all the necessary features. This
 * implementation is provided solely for basic testing purposes.
 */

public class NoOpTimelineReaderImpl extends AbstractService
        implements TimelineReader {
  private static final Logger LOG =
          LoggerFactory.getLogger(NoOpTimelineReaderImpl.class);

  public NoOpTimelineReaderImpl() {
    super(NoOpTimelineReaderImpl.class.getName());
    LOG.info("NoOpTimelineReader is configured. Response to all the read " +
            "requests would be empty");
  }

  @Override
  public TimelineEntity getEntity(TimelineReaderContext context,
       TimelineDataToRetrieve dataToRetrieve) throws IOException {
    LOG.debug("NoOpTimelineReader is configured. Response to all the read " +
              "requests would be empty");
    return new TimelineEntity();
  }

  @Override
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
          TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
          throws IOException {
    LOG.debug("NoOpTimelineReader is configured. Response to all the read " +
              "requests would be empty");
    return new HashSet<>();
  }

  @Override
  public Set<String> getEntityTypes(TimelineReaderContext context)
          throws IOException {
    LOG.debug("NoOpTimelineReader is configured. Response to all the read " +
              "requests would be empty");
    return new HashSet<>();
  }

  @Override
  public TimelineHealth getHealthStatus() {
    return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING,
        "NoOpTimelineReader is configured. ");
  }
}
