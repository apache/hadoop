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
import { extractPlacementRulesFromConfig } from './placementRulesUtils';
import { SPECIAL_VALUES } from '~/types/constants/special-values';

describe('extractPlacementRulesFromConfig', () => {
  it('should return JSON format rules when configured', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.mapping-rule-format', 'json'],
      [
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        JSON.stringify({
          rules: [
            {
              type: 'user',
              matches: '*',
              policy: 'user',
              parentQueue: 'root.users',
              fallbackResult: 'skip',
            },
            {
              type: 'group',
              matches: 'dev',
              policy: 'specified',
              value: 'root.dev',
              create: true,
            },
          ],
        }),
      ],
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toHaveLength(2);
    expect(result.rules?.[0]).toEqual({
      type: 'user',
      matches: '*',
      policy: 'user',
      parentQueue: 'root.users',
      fallbackResult: 'skip',
    });
    expect(result.rules?.[1]).toEqual({
      type: 'group',
      matches: 'dev',
      policy: 'specified',
      value: 'root.dev',
      create: true,
    });
  });

  it('should return empty rules array when JSON format but no rules property', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.mapping-rule-format', 'json'],
      [SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, JSON.stringify({})],
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toEqual([]);
  });

  it('should return empty rules array when JSON format but no JSON property', () => {
    const configData = new Map([['yarn.scheduler.capacity.mapping-rule-format', 'json']]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toEqual([]);
  });

  it('should handle invalid JSON gracefully', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.mapping-rule-format', 'json'],
      [SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, 'invalid json'],
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toEqual([]);
  });

  it('should detect legacy format rules', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.queue-mappings', 'u:user1:queue1,g:group1:queue2'],
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('legacy');
    expect(result.legacyRules).toBe('u:user1:queue1,g:group1:queue2');
    expect(result.requiresMigration).toBe(true);
    expect(result.rules).toBeUndefined();
  });

  it('should return none format when no rules configured', () => {
    const configData = new Map([['some.other.property', 'value']]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('none');
    expect(result.rules).toBeUndefined();
    expect(result.legacyRules).toBeUndefined();
    expect(result.requiresMigration).toBeUndefined();
  });

  it('should handle empty config map', () => {
    const configData = new Map();

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('none');
    expect(result.rules).toBeUndefined();
    expect(result.legacyRules).toBeUndefined();
  });

  it('should prioritize JSON format over legacy when both exist', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.mapping-rule-format', 'json'],
      [
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        JSON.stringify({
          rules: [{ type: 'user', matches: '*', policy: 'user' }],
        }),
      ],
      ['yarn.scheduler.capacity.queue-mappings', 'u:user1:queue1'], // This should be ignored
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toHaveLength(1);
    expect(result.legacyRules).toBeUndefined();
  });

  it('should auto-detect JSON rules when format is legacy but no queue-mappings exist', () => {
    const configData = new Map([
      ['yarn.scheduler.capacity.mapping-rule-format', 'legacy'],
      [
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        JSON.stringify({
          rules: [
            { type: 'user', matches: '*', policy: 'user', parentQueue: 'root.users' },
            { type: 'group', matches: 'dev', policy: 'specified', value: 'root.dev' },
          ],
        }),
      ],
      // Note: no queue-mappings property
    ]);

    const result = extractPlacementRulesFromConfig(configData);

    expect(result.format).toBe('json');
    expect(result.rules).toHaveLength(2);
    expect(result.inconsistentFormat).toBe(true);
    expect(result.legacyRules).toBeUndefined();
  });
});
