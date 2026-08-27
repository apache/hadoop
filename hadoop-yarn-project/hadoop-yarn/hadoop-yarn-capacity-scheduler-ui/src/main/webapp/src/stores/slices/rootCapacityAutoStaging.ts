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

import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { getCapacityType } from '~/utils/capacityUtils';

export interface RootCapacityAutoStagingResult {
  capacity: string;
  direction: 'to-weight' | 'to-percentage';
}

export interface RootCapacityAutoStagingContext {
  queuePath: string;
  changedData: Record<string, string>;
  getQueuePropertyValue: (
    queuePath: string,
    propertyName: string,
  ) => { value: string; isStaged: boolean };
  configData: Map<string, string>;
}

function canAutoStageRootCapacity({
  queuePath,
  changedData,
  configData,
}: RootCapacityAutoStagingContext): boolean {
  if (queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME) {
    return false;
  }

  if (changedData.capacity !== undefined) {
    return false;
  }

  return configData.get(SPECIAL_VALUES.LEGACY_MODE_PROPERTY) !== 'false';
}

function resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation(
  options: RootCapacityAutoStagingContext,
): RootCapacityAutoStagingResult | null {
  if (!canAutoStageRootCapacity(options)) {
    return null;
  }

  if (options.changedData[AUTO_CREATION_PROPS.FLEXIBLE_ENABLED] !== 'true') {
    return null;
  }

  const { value: currentCapacity } = options.getQueuePropertyValue(options.queuePath, 'capacity');
  if (getCapacityType(currentCapacity) !== 'percentage') {
    return null;
  }

  return {
    capacity: '1w',
    direction: 'to-weight',
  };
}

function resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation(
  options: RootCapacityAutoStagingContext,
): RootCapacityAutoStagingResult | null {
  if (!canAutoStageRootCapacity(options)) {
    return null;
  }

  const flexibleEnabledChange = options.changedData[AUTO_CREATION_PROPS.FLEXIBLE_ENABLED];
  if (flexibleEnabledChange === undefined || flexibleEnabledChange === 'true') {
    return null;
  }

  const { value: currentFlexibleEnabled } = options.getQueuePropertyValue(
    options.queuePath,
    AUTO_CREATION_PROPS.FLEXIBLE_ENABLED,
  );
  if (currentFlexibleEnabled !== 'true') {
    return null;
  }

  const { value: currentCapacity } = options.getQueuePropertyValue(options.queuePath, 'capacity');
  if (getCapacityType(currentCapacity) !== 'weight') {
    return null;
  }

  return {
    capacity: '100%',
    direction: 'to-percentage',
  };
}

/**
 * Restore root capacity when flexible auto-queue-creation is toggled.
 * Convert 100% -> 1w when the toggle is turned on.
 * Convert 1w -> 100% when the toggle is turned off.
 */
export function resolveRootCapacityStagingWhenAutoQueueCreationIsToggled(
  context: RootCapacityAutoStagingContext,
): RootCapacityAutoStagingResult | null {
  return (
    resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation(context) ??
    resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation(context)
  );
}
