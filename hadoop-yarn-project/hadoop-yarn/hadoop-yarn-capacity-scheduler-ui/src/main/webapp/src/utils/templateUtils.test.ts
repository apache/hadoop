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

import { describe, expect, it } from 'vitest';

import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';

import {
  getAutoCreatedQueueTemplatePath,
  resolveAutoCreatedQueueCapacityConfigs,
  TEMPLATE_SUFFIXES,
} from './templateUtils';

const createLookup =
  (config: Record<string, Record<string, string>>) =>
  (queuePath: string, property: string) => ({
    value: config[queuePath]?.[property] ?? '',
    isStaged: false,
  });

describe('templateUtils auto-created queue capacity', () => {
  describe('getAutoCreatedQueueTemplatePath', () => {
    it('uses leaf-template when flexible auto-creation v2 is enabled on parent', () => {
      const getQueuePropertyValue = createLookup({
        'root.default': {
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
        },
      });

      expect(getAutoCreatedQueueTemplatePath('root.default.user1', getQueuePropertyValue)).toBe(
        `root.default.${TEMPLATE_SUFFIXES.FLEXIBLE_LEAF}`,
      );
    });

    it('uses leaf-queue-template when legacy auto-creation is enabled on parent', () => {
      const getQueuePropertyValue = createLookup({
        'root.default': {
          [AUTO_CREATION_PROPS.LEGACY_ENABLED]: 'true',
        },
      });

      expect(getAutoCreatedQueueTemplatePath('root.default.user1', getQueuePropertyValue)).toBe(
        `root.default.${TEMPLATE_SUFFIXES.LEGACY}`,
      );
    });

    it('returns null when neither auto-creation mode is enabled on parent', () => {
      expect(getAutoCreatedQueueTemplatePath('root.default.user1', createLookup({}))).toBeNull();
    });
  });

  describe('resolveAutoCreatedQueueCapacityConfigs', () => {
    it('uses leaf-template capacity for flexible auto-creation (weight mode)', () => {
      const getQueuePropertyValue = createLookup({
        'root.default': {
          [AUTO_CREATION_PROPS.FLEXIBLE_ENABLED]: 'true',
        },
        'root.default.user1': {
          capacity: '0',
        },
        [`root.default.${TEMPLATE_SUFFIXES.FLEXIBLE_LEAF}`]: {
          capacity: '3w',
          'maximum-capacity': '100',
        },
      });

      const resolved = resolveAutoCreatedQueueCapacityConfigs(
        'root.default.user1',
        'dynamicFlexible',
        getQueuePropertyValue,
      );

      expect(resolved.capacityConfig).toBe('3w');
      expect(resolved.maxCapacityConfig).toBe('100');
    });

    it('uses leaf-queue-template capacity for legacy auto-creation (relative/absolute mode)', () => {
      const getQueuePropertyValue = createLookup({
        'root.default': {
          [AUTO_CREATION_PROPS.LEGACY_ENABLED]: 'true',
        },
        'root.default.user1': {
          capacity: '0',
        },
        [`root.default.${TEMPLATE_SUFFIXES.LEGACY}`]: {
          capacity: '100',
          'maximum-capacity': '100',
        },
        [`root.default.${TEMPLATE_SUFFIXES.FLEXIBLE_LEAF}`]: {
          capacity: '3w',
        },
      });

      const resolved = resolveAutoCreatedQueueCapacityConfigs(
        'root.default.user1',
        'dynamicLegacy',
        getQueuePropertyValue,
      );

      expect(resolved.capacityConfig).toBe('100');
      expect(resolved.maxCapacityConfig).toBe('100');
    });

    it('keeps configured capacity for static queues', () => {
      const getQueuePropertyValue = createLookup({
        'root.default': {
          capacity: '25',
          'maximum-capacity': '100',
        },
      });

      const resolved = resolveAutoCreatedQueueCapacityConfigs(
        'root.default',
        'static',
        getQueuePropertyValue,
      );

      expect(resolved.capacityConfig).toBe('25');
      expect(resolved.maxCapacityConfig).toBe('100');
    });
  });
});
