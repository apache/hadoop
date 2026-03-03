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
  formatAclValue,
  formatPropertyName,
  formatMemory,
  formatPercentage,
  formatCount,
} from './formatUtils';
import { SPECIAL_VALUES } from '~/types/constants/special-values';

describe('formatAclValue', () => {
  it('should format all users ACL value', () => {
    expect(formatAclValue('*')).toBe('* (All users)');
    expect(formatAclValue(SPECIAL_VALUES.ALL_USERS_ACL)).toBe('* (All users)');
  });

  it('should format no users ACL value', () => {
    expect(formatAclValue(' ')).toBe('" " (No access)');
    expect(formatAclValue(SPECIAL_VALUES.NO_USERS_ACL)).toBe('" " (No access)');
  });

  it('should format empty value', () => {
    expect(formatAclValue('')).toBe('(empty)');
    expect(formatAclValue(undefined)).toBe('(empty)');
  });

  it('should pass through regular ACL values', () => {
    expect(formatAclValue('user1,user2 group1,group2')).toBe('user1,user2 group1,group2');
    expect(formatAclValue('admin')).toBe('admin');
    expect(formatAclValue('user1,user2')).toBe('user1,user2');
  });
});

describe('formatPropertyName', () => {
  it('should format regular property names', () => {
    expect(formatPropertyName('maximum-capacity')).toBe('Maximum Capacity');
    expect(formatPropertyName('auto-create-child-queue')).toBe('Auto Create Child Queue');
  });

  it('should format queue marker', () => {
    expect(formatPropertyName(SPECIAL_VALUES.QUEUE_MARKER)).toBe('Queue removal');
  });

  it('should format node label properties', () => {
    expect(formatPropertyName('accessible-node-labels.gpu.capacity')).toBe('Capacity (label: gpu)');
    expect(formatPropertyName('accessible-node-labels.hdd.maximum-capacity')).toBe(
      'Maximum Capacity (label: hdd)',
    );
  });

  it('should handle undefined property', () => {
    expect(formatPropertyName(undefined)).toBe('Queue operation');
  });
});

describe('formatMemory', () => {
  it('should format memory in MB', () => {
    expect(formatMemory(512)).toBe('512 MB');
    expect(formatMemory(100)).toBe('100 MB');
  });

  it('should format memory in GB', () => {
    expect(formatMemory(1024)).toBe('1 GB');
    expect(formatMemory(2048)).toBe('2 GB');
    expect(formatMemory(1536)).toBe('1.5 GB');
  });
});

describe('formatPercentage', () => {
  it('should format percentage with default decimals', () => {
    expect(formatPercentage(45.5)).toBe('45.5%');
    expect(formatPercentage(100)).toBe('100.0%');
  });

  it('should format percentage with custom decimals', () => {
    expect(formatPercentage(45.567, 2)).toBe('45.57%');
    expect(formatPercentage(100, 0)).toBe('100%');
  });
});

describe('formatCount', () => {
  it('should format numbers with thousand separators', () => {
    expect(formatCount(1000)).toBe('1,000');
    expect(formatCount(1234567)).toBe('1,234,567');
    expect(formatCount(100)).toBe('100');
  });
});
