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
  parseCapacityValue,
  getCapacityType,
  isVectorCapacity,
  parseResourceVector,
} from './capacityUtils';

describe('capacityUtils', () => {
  describe('parseCapacityValue', () => {
    it('should parse percentage values', () => {
      const result = parseCapacityValue('50%');
      expect(result).toEqual({
        type: 'percentage',
        value: 50,
        rawValue: '50%',
      });
    });

    it('should parse percentage values without % symbol', () => {
      const result = parseCapacityValue('75');
      expect(result).toEqual({
        type: 'percentage',
        value: 75,
        rawValue: '75',
      });
    });

    it('should parse weight values', () => {
      const result = parseCapacityValue('2.5w');
      expect(result).toEqual({
        type: 'weight',
        value: 2.5,
        rawValue: '2.5w',
      });
    });

    it('should parse absolute values', () => {
      const result = parseCapacityValue('[memory=1024,vcores=2]');
      expect(result).toEqual({
        type: 'absolute',
        value: 0,
        resources: {
          memory: 1024,
          vcores: 2,
        },
        rawValue: '[memory=1024,vcores=2]',
      });
    });

    it('should handle -1 as 100%', () => {
      const result = parseCapacityValue('-1');
      expect(result).toEqual({
        type: 'percentage',
        value: 100,
        rawValue: '-1',
      });
    });

    it('should return null for invalid values', () => {
      expect(parseCapacityValue('')).toBeNull();
      expect(parseCapacityValue('invalid')).toBeNull();
      expect(parseCapacityValue('0w')).toBeNull();
      expect(parseCapacityValue('-5w')).toBeNull();
      expect(parseCapacityValue('[]')).toBeNull();
      expect(parseCapacityValue('[invalid]')).toBeNull();
    });

    it('should handle edge cases', () => {
      expect(parseCapacityValue('0%')).toEqual({
        type: 'percentage',
        value: 0,
        rawValue: '0%',
      });

      expect(parseCapacityValue('100%')).toEqual({
        type: 'percentage',
        value: 100,
        rawValue: '100%',
      });

      expect(parseCapacityValue('0.001w')).toEqual({
        type: 'weight',
        value: 0.001,
        rawValue: '0.001w',
      });
    });
  });

  describe('getCapacityType', () => {
    it('should return correct capacity type', () => {
      expect(getCapacityType('50%')).toBe('percentage');
      expect(getCapacityType('2w')).toBe('weight');
      expect(getCapacityType('[memory=1024]')).toBe('absolute');
      expect(getCapacityType('invalid')).toBeNull();
    });
  });

  describe('isVectorCapacity', () => {
    it('should return true for vector format', () => {
      expect(isVectorCapacity('[memory=1024,vcores=2]')).toBe(true);
      expect(isVectorCapacity('[memory=1024]')).toBe(true);
      expect(isVectorCapacity('[]')).toBe(true);
    });

    it('should return false for non-vector formats', () => {
      expect(isVectorCapacity('50%')).toBe(false);
      expect(isVectorCapacity('2w')).toBe(false);
      expect(isVectorCapacity('100')).toBe(false);
      expect(isVectorCapacity('memory=1024')).toBe(false);
      expect(isVectorCapacity('[incomplete')).toBe(false);
      expect(isVectorCapacity('incomplete]')).toBe(false);
    });

    it('should handle null and undefined', () => {
      expect(isVectorCapacity(null)).toBe(false);
      expect(isVectorCapacity(undefined)).toBe(false);
      expect(isVectorCapacity('')).toBe(false);
    });

    it('should handle whitespace', () => {
      expect(isVectorCapacity('  [memory=1024]  ')).toBe(true);
      expect(isVectorCapacity('  50%  ')).toBe(false);
    });
  });

  describe('parseResourceVector', () => {
    it('should parse valid vector strings', () => {
      const result = parseResourceVector('[memory=1024,vcores=2]');
      expect(result).toEqual([
        { resource: 'memory', value: '1024' },
        { resource: 'vcores', value: '2' },
      ]);
    });

    it('should parse single entry vectors', () => {
      const result = parseResourceVector('[memory=1024]');
      expect(result).toEqual([{ resource: 'memory', value: '1024' }]);
    });

    it('should handle whitespace in vectors', () => {
      const result = parseResourceVector('[ memory = 1024 , vcores = 2 ]');
      expect(result).toEqual([
        { resource: 'memory', value: '1024' },
        { resource: 'vcores', value: '2' },
      ]);
    });

    it('should return empty array for non-vector strings', () => {
      expect(parseResourceVector('50%')).toEqual([]);
      expect(parseResourceVector('2w')).toEqual([]);
      expect(parseResourceVector('invalid')).toEqual([]);
    });

    it('should return empty array for empty vectors', () => {
      expect(parseResourceVector('[]')).toEqual([]);
      expect(parseResourceVector('[  ]')).toEqual([]);
    });

    it('should filter out invalid entries', () => {
      // Missing value - filtered out
      const result = parseResourceVector('[memory=1024,vcores=]');
      expect(result).toEqual([{ resource: 'memory', value: '1024' }]);
    });

    it('should filter out entries without equals sign', () => {
      const result = parseResourceVector('[memory=1024,vcores]');
      expect(result).toEqual([{ resource: 'memory', value: '1024' }]);
    });
  });
});
