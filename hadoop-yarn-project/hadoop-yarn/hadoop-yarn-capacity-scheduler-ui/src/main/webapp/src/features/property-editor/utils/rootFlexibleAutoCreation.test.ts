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

import { describe, it, expect } from 'vitest';
import {
  resolveRootCapacityStagingForFlexibleAutoCreation,
  resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation,
  resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation,
  ROOT_FLEXIBLE_AUTO_CREATION_WEIGHT_CAPACITY,
  ROOT_FLEXIBLE_AUTO_CREATION_PERCENTAGE_CAPACITY,
} from './rootFlexibleAutoCreation';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import { SPECIAL_VALUES } from '~/types/constants/special-values';

describe('root flexible auto-creation capacity staging', () => {
  const createGetQueuePropertyValue =
    (values: Record<string, string>) => (_queuePath: string, propertyName: string) => ({
      value: values[propertyName] ?? '',
      isStaged: false,
    });

  const legacyConfig = new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'true']]);

  describe('resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation', () => {
    it('returns 1w when enabling on root with percentage capacity', () => {
      const result = resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '100' }),
        configData: legacyConfig,
      });

      expect(result).toEqual({
        capacity: ROOT_FLEXIBLE_AUTO_CREATION_WEIGHT_CAPACITY,
        direction: 'to-weight',
      });
    });

    it('returns null when root already uses weight capacity', () => {
      const result = resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '2w' }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });

    it('returns null when disabling flexible auto-creation', () => {
      const result = resolveRootCapacityStagingWhenEnablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '100' }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });
  });

  describe('resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation', () => {
    it('returns 100 when disabling on root with weight capacity', () => {
      const result = resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false' },
        getQueuePropertyValue: createGetQueuePropertyValue({
          capacity: '1w',
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
        }),
        configData: legacyConfig,
      });

      expect(result).toEqual({
        capacity: ROOT_FLEXIBLE_AUTO_CREATION_PERCENTAGE_CAPACITY,
        direction: 'to-percentage',
      });
    });

    it('returns null when flexible auto-creation was not enabled', () => {
      const result = resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false' },
        getQueuePropertyValue: createGetQueuePropertyValue({
          capacity: '1w',
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false',
        }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });

    it('returns null when root already uses percentage capacity', () => {
      const result = resolveRootCapacityStagingWhenDisablingFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false' },
        getQueuePropertyValue: createGetQueuePropertyValue({
          capacity: '100',
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
        }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });
  });

  describe('resolveRootCapacityStagingForFlexibleAutoCreation', () => {
    it('delegates to the enable handler when turning flexible auto-creation on', () => {
      const result = resolveRootCapacityStagingForFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '100' }),
        configData: legacyConfig,
      });

      expect(result?.direction).toBe('to-weight');
    });

    it('delegates to the disable handler when turning flexible auto-creation off', () => {
      const result = resolveRootCapacityStagingForFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'false' },
        getQueuePropertyValue: createGetQueuePropertyValue({
          capacity: '1w',
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
        }),
        configData: legacyConfig,
      });

      expect(result?.direction).toBe('to-percentage');
    });

    it('returns null for non-root queues', () => {
      const result = resolveRootCapacityStagingForFlexibleAutoCreation({
        queuePath: 'root.default',
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '2w' }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });

    it('returns null when legacy mode is disabled', () => {
      const result = resolveRootCapacityStagingForFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: { [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true' },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '100' }),
        configData: new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'false']]),
      });

      expect(result).toBeNull();
    });

    it('returns null when capacity is already being changed explicitly', () => {
      const result = resolveRootCapacityStagingForFlexibleAutoCreation({
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        changedData: {
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
          capacity: '2w',
        },
        getQueuePropertyValue: createGetQueuePropertyValue({ capacity: '100' }),
        configData: legacyConfig,
      });

      expect(result).toBeNull();
    });
  });
});
