/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.skylinestore.validator;

import java.util.List;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullPipelineIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullRLESparseResourceAllocationException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullRecurrenceIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.NullResourceSkylineException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SkylineStoreValidator validates input parameters for {@link SkylineStore}.
 */
public class SkylineStoreValidator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SkylineStoreValidator.class);

  /**
   * Check if recurrenceId is <em>null</em>.
   *
   * @param recurrenceId the id of the recurring pipeline job.
   * @throws SkylineStoreException if input parameters are invalid.
   */
  public final void validate(final RecurrenceId recurrenceId)
      throws SkylineStoreException {
    if (recurrenceId == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Recurrence id is null, please try again by specifying"
          + " a valid Recurrence id.");
      LOGGER.error(sb.toString());
      throw new NullRecurrenceIdException(sb.toString());
    }
  }

  /**
   * Check if pipelineId is <em>null</em>.
   *
   * @param pipelineId the id of the recurring pipeline job.
   * @throws SkylineStoreException if input parameters are invalid.
   */
  public final void validate(final String pipelineId)
      throws SkylineStoreException {
    if (pipelineId == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("pipelineId is null, please try again by specifying"
          + " a valid pipelineId.");
      LOGGER.error(sb.toString());
      throw new NullPipelineIdException(sb.toString());
    }
  }

  /**
   * Check if recurrenceId is <em>null</em> or resourceSkylines is
   * <em>null</em>.
   *
   * @param recurrenceId     the id of the recurring pipeline job.
   * @param resourceSkylines the list of {@link ResourceSkyline}s to be added.
   * @throws SkylineStoreException if input parameters are invalid.
   */
  public final void validate(final RecurrenceId recurrenceId,
      final List<ResourceSkyline> resourceSkylines)
      throws SkylineStoreException {
    validate(recurrenceId);
    if (resourceSkylines == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("ResourceSkylines for " + recurrenceId
          + " is null, please try again by "
          + "specifying valid ResourceSkylines.");
      LOGGER.error(sb.toString());
      throw new NullResourceSkylineException(sb.toString());
    }
  }

  /**
   * Check if pipelineId is <em>null</em> or resourceOverTime is <em>null</em>.
   *
   * @param pipelineId       the id of the recurring pipeline.
   * @param resourceOverTime predicted {@code Resource} allocation to be added.
   * @throws SkylineStoreException if input parameters are invalid.
   */
  public final void validate(final String pipelineId,
      final RLESparseResourceAllocation resourceOverTime)
      throws SkylineStoreException {
    validate(pipelineId);
    if (resourceOverTime == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Resource allocation for " + pipelineId + " is null.");
      LOGGER.error(sb.toString());
      throw new NullRLESparseResourceAllocationException(sb.toString());
    }
  }
}
