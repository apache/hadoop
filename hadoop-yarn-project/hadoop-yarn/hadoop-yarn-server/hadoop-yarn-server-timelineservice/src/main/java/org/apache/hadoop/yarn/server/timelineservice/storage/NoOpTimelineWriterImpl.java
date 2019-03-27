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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Stub based implementation for TimelineWriter. This implementation will
 * not provide a complete implementation of all the necessary features. This
 * implementation is provided solely for basic testing purposes.
 */

public class NoOpTimelineWriterImpl extends AbstractService implements
                                                      TimelineWriter{
  private static final Logger LOG =
          LoggerFactory.getLogger(NoOpTimelineWriterImpl.class);

  public NoOpTimelineWriterImpl() {
    super(NoOpTimelineWriterImpl.class.getName());
    LOG.info("NoOpTimelineWriter is configured. All the writes to the backend" +
            " are ignored");
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
                                     TimelineEntities data,
                                     UserGroupInformation callerUgi)
                                     throws IOException {
    LOG.debug("NoOpTimelineWriter is configured. Not storing " +
              "TimelineEntities.");
    return new TimelineWriteResponse();
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
                                     TimelineDomain domain) throws IOException {
    LOG.debug("NoOpTimelineWriter is configured. Not storing " +
              "TimelineEntities.");
    return new TimelineWriteResponse();
  }

  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
                                         TimelineAggregationTrack track)
                                         throws IOException {
    LOG.debug("NoOpTimelineWriter is configured. Not aggregating " +
              "TimelineEntities.");
    return new TimelineWriteResponse();
  }

  @Override
  public void flush() throws IOException {
    LOG.debug("NoOpTimelineWriter is configured. Ignoring flush call");
  }
}
