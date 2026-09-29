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
import { migrateLegacyRules } from './migration';

describe('migrateLegacyRules', () => {
  it('should return empty rules for empty input', () => {
    const result = migrateLegacyRules('');
    expect(result.success).toBe(true);
    expect(result.rules).toEqual([]);
    expect(result.errors).toEqual([]);
  });

  it('should return empty rules for whitespace-only input', () => {
    const result = migrateLegacyRules('   \n\t  ');
    expect(result.success).toBe(true);
    expect(result.rules).toEqual([]);
    expect(result.errors).toEqual([]);
  });

  describe('User Rules', () => {
    it('should convert simple user rule', () => {
      const result = migrateLegacyRules('u:alice:root.users.alice');
      expect(result.success).toBe(true);
      expect(result.rules).toHaveLength(1);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'alice',
        policy: 'custom',
        customPlacement: 'root.users.alice',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert dynamic user rule with %user', () => {
      const result = migrateLegacyRules('u:%user:root.users.%user');
      expect(result.success).toBe(true);
      expect(result.rules).toHaveLength(1);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: '*',
        policy: 'user',
        parentQueue: 'root.users',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert user rule with primary group', () => {
      const result = migrateLegacyRules('u:bob:root.groups.%primary_group');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'bob',
        policy: 'primaryGroup',
        parentQueue: 'root.groups',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert user rule with secondary group', () => {
      const result = migrateLegacyRules('u:charlie:root.groups.%secondary_group');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'charlie',
        policy: 'secondaryGroup',
        parentQueue: 'root.groups',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert complex user rule with primaryGroupUser pattern', () => {
      const result = migrateLegacyRules('u:%user:root.dept.%primary_group.%user');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: '*',
        policy: 'primaryGroupUser',
        parentQueue: 'root.dept',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert complex user rule with secondaryGroupUser pattern', () => {
      const result = migrateLegacyRules('u:dave:root.teams.%secondary_group.%user');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'dave',
        policy: 'secondaryGroupUser',
        parentQueue: 'root.teams',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });
  });

  describe('Group Rules', () => {
    it('should convert simple group rule', () => {
      const result = migrateLegacyRules('g:developers:root.teams.dev');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'group',
        matches: 'developers',
        policy: 'custom',
        customPlacement: 'root.teams.dev',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert group rule with primary group variable', () => {
      const result = migrateLegacyRules('g:staff:root.groups.%primary_group');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'group',
        matches: 'staff',
        policy: 'primaryGroup',
        parentQueue: 'root.groups',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });
  });

  describe('Application Rules', () => {
    it('should convert simple application rule', () => {
      const result = migrateLegacyRules('mapreduce:root.apps.mapreduce');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'application',
        matches: 'mapreduce',
        policy: 'custom',
        customPlacement: 'root.apps.mapreduce',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert application rule with pattern matching', () => {
      const result = migrateLegacyRules('spark-*:root.apps.spark');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'application',
        matches: 'spark-*',
        policy: 'custom',
        customPlacement: 'root.apps.spark',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });
  });

  describe('Multiple Rules', () => {
    it('should convert multiple rules separated by commas', () => {
      const result = migrateLegacyRules(
        'u:alice:root.users.alice,g:developers:root.teams.dev,mapreduce:root.apps.mr',
      );
      expect(result.success).toBe(true);
      expect(result.rules).toHaveLength(3);
      expect(result.rules[0].type).toBe('user');
      expect(result.rules[1].type).toBe('group');
      expect(result.rules[2].type).toBe('application');
    });

    it('should handle extra spaces and empty entries', () => {
      const result = migrateLegacyRules(
        ' u:alice:root.users.alice , , g:developers:root.teams.dev ',
      );
      expect(result.success).toBe(true);
      expect(result.rules).toHaveLength(2);
    });
  });

  describe('Edge Cases', () => {
    it('should handle queue names with colons', () => {
      const result = migrateLegacyRules('u:alice:root.queue:with:colons');
      expect(result.success).toBe(true);
      expect(result.rules[0].customPlacement).toBe('root.queue:with:colons');
    });

    it('should handle single-level queue paths', () => {
      const result = migrateLegacyRules('u:alice:default');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'alice',
        policy: 'custom',
        customPlacement: 'default',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should convert user rule with no parent queue', () => {
      const result = migrateLegacyRules('u:alice:%user');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'alice',
        policy: 'user',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });
  });

  describe('Error Handling', () => {
    it('should handle invalid rule format', () => {
      const result = migrateLegacyRules('invalid_rule');
      expect(result.success).toBe(false);
      expect(result.rules).toHaveLength(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]).toContain('Failed to convert rule "invalid_rule"');
      expect(result.errors[0]).toContain('Invalid rule format');
    });

    it('should handle empty parts', () => {
      const result = migrateLegacyRules('u::root.users');
      expect(result.success).toBe(false);
      expect(result.errors[0]).toContain('Missing matcher or target queue');
    });

    it('should handle multiple errors', () => {
      const result = migrateLegacyRules('invalid1,u:alice:root.users.alice,invalid2');
      expect(result.success).toBe(false);
      expect(result.rules).toHaveLength(1); // One valid rule
      expect(result.errors).toHaveLength(2); // Two invalid rules
    });

    it('should continue processing after encountering errors', () => {
      const result = migrateLegacyRules('invalid,u:alice:root.users.alice,g:dev:root.teams.dev');
      expect(result.success).toBe(false);
      expect(result.rules).toHaveLength(2); // Two valid rules
      expect(result.errors).toHaveLength(1); // One invalid rule
      expect(result.rules[0].matches).toBe('alice');
      expect(result.rules[1].matches).toBe('dev');
    });
  });

  describe('Special Patterns', () => {
    it('should handle %application in user rules as custom', () => {
      const result = migrateLegacyRules('u:alice:root.apps.%application');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'user',
        matches: 'alice',
        policy: 'custom',
        customPlacement: 'root.apps.%application',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });

    it('should handle %application in application rules as defaultQueue', () => {
      const result = migrateLegacyRules('spark:root.apps.%application');
      expect(result.success).toBe(true);
      expect(result.rules[0]).toEqual({
        type: 'application',
        matches: 'spark',
        policy: 'defaultQueue',
        fallbackResult: 'placeDefault',
        create: true,
      });
    });
  });
});
