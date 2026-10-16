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
import { z } from 'zod';
import {
  PlacementRuleSchema,
  isPlacementRule,
  validatePlacementRule,
  isValidPlacementRule,
} from './validators';
import type { PlacementRule } from './index';

describe('PlacementRule Validators', () => {
  const validRule: PlacementRule = {
    type: 'user',
    matches: '*',
    policy: 'specified',
    parentQueue: 'root.users',
    value: 'default',
    create: true,
    fallbackResult: 'skip',
  };

  describe('PlacementRuleSchema', () => {
    it('should validate a complete placement rule', () => {
      const result = PlacementRuleSchema.parse(validRule);
      expect(result).toEqual(validRule);
    });

    it('should validate a minimal placement rule', () => {
      const minimalRule = {
        type: 'group',
        matches: 'dev',
        policy: 'reject',
      };
      const result = PlacementRuleSchema.parse(minimalRule);
      expect(result).toEqual(minimalRule);
    });

    it('should reject invalid type', () => {
      const invalidRule = { ...validRule, type: 'invalid' };
      expect(() => PlacementRuleSchema.parse(invalidRule)).toThrow(z.ZodError);
    });

    it('should reject empty matches', () => {
      const invalidRule = { ...validRule, matches: '' };
      expect(() => PlacementRuleSchema.parse(invalidRule)).toThrow('Matches pattern is required');
    });

    it('should reject invalid policy', () => {
      const invalidRule = { ...validRule, policy: 'invalid' };
      expect(() => PlacementRuleSchema.parse(invalidRule)).toThrow(z.ZodError);
    });

    it('should reject invalid fallbackResult', () => {
      const invalidRule = { ...validRule, fallbackResult: 'invalid' as any };
      expect(() => PlacementRuleSchema.parse(invalidRule)).toThrow(z.ZodError);
    });

    it('should accept all valid rule types', () => {
      const types: Array<PlacementRule['type']> = ['user', 'group', 'application'];
      types.forEach((type) => {
        const rule = { ...validRule, type };
        expect(() => PlacementRuleSchema.parse(rule)).not.toThrow();
      });
    });

    it('should accept all valid policies', () => {
      const policies: Array<PlacementRule['policy']> = [
        'specified',
        'primaryGroup',
        'primaryGroupUser',
        'secondaryGroup',
        'secondaryGroupUser',
        'reject',
        'defaultQueue',
        'user',
        'custom',
        'setDefaultQueue',
      ];
      policies.forEach((policy) => {
        const rule = { ...validRule, policy };
        expect(() => PlacementRuleSchema.parse(rule)).not.toThrow();
      });
    });

    it('should accept all valid fallback results', () => {
      const fallbackResults: Array<PlacementRule['fallbackResult']> = [
        'skip',
        'placeDefault',
        'reject',
      ];
      fallbackResults.forEach((fallbackResult) => {
        const rule = { ...validRule, fallbackResult };
        expect(() => PlacementRuleSchema.parse(rule)).not.toThrow();
      });
    });
  });

  describe('isPlacementRule', () => {
    it('should return true for valid placement rules', () => {
      expect(isPlacementRule(validRule)).toBe(true);
      expect(
        isPlacementRule({
          type: 'group',
          matches: 'dev',
          policy: 'primaryGroup',
        }),
      ).toBe(true);
    });

    it('should return false for invalid data', () => {
      expect(isPlacementRule(null)).toBe(false);
      expect(isPlacementRule(undefined)).toBe(false);
      expect(isPlacementRule({})).toBe(false);
      expect(isPlacementRule({ type: 'user' })).toBe(false); // missing required fields
      expect(isPlacementRule({ type: 'invalid', matches: '*', policy: 'user' })).toBe(false);
      expect(isPlacementRule('not an object')).toBe(false);
      expect(isPlacementRule(123)).toBe(false);
      expect(isPlacementRule([])).toBe(false);
    });

    it('should not throw for any input', () => {
      const testCases = [null, undefined, {}, [], 'string', 123, true, Symbol('test')];
      testCases.forEach((testCase) => {
        expect(() => isPlacementRule(testCase)).not.toThrow();
      });
    });
  });

  describe('validatePlacementRule', () => {
    it('should return parsed rule for valid data', () => {
      const result = validatePlacementRule(validRule);
      expect(result).toEqual(validRule);
    });

    it('should handle extra properties by stripping them', () => {
      const ruleWithExtra = { ...validRule, extraProp: 'should be removed' };
      const result = validatePlacementRule(ruleWithExtra);
      expect(result).toEqual(validRule);
      expect('extraProp' in result).toBe(false);
    });

    it('should throw ZodError for invalid data', () => {
      expect(() => validatePlacementRule({})).toThrow(z.ZodError);
      expect(() => validatePlacementRule(null)).toThrow(z.ZodError);
      expect(() => validatePlacementRule({ type: 'user' })).toThrow(z.ZodError);
    });

    it('should provide helpful error messages', () => {
      try {
        validatePlacementRule({ type: 'user', matches: '', policy: 'invalid' });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(z.ZodError);
        const zodError = error as z.ZodError;
        const issues = zodError.issues.map((i) => i.message);
        expect(issues).toContain('Matches pattern is required');
        expect(issues.some((msg) => msg.includes('Invalid enum value'))).toBe(true);
      }
    });
  });

  describe('isValidPlacementRule', () => {
    it('should return valid: true with data for valid rules', () => {
      const result = isValidPlacementRule(validRule);
      expect(result.valid).toBe(true);
      if (result.valid) {
        expect(result.data).toEqual(validRule);
      }
    });

    it('should return valid: false with error for invalid rules', () => {
      const result = isValidPlacementRule({});
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.error).toBeInstanceOf(z.ZodError);
        expect(result.error.issues.length).toBeGreaterThan(0);
      }
    });

    it('should handle all types of invalid input gracefully', () => {
      const invalidInputs = [
        null,
        undefined,
        'string',
        123,
        [],
        { type: 'invalid', matches: '*', policy: 'user' },
        { type: 'user' }, // missing required fields
      ];

      invalidInputs.forEach((input) => {
        const result = isValidPlacementRule(input);
        expect(result.valid).toBe(false);
        if (!result.valid) {
          expect(result.error).toBeInstanceOf(z.ZodError);
        }
      });
    });

    it('should provide detailed error information', () => {
      const result = isValidPlacementRule({
        type: 'user',
        matches: '', // empty string
        policy: 'invalid', // invalid enum value
      });

      expect(result.valid).toBe(false);
      if (!result.valid) {
        const errorMessages = result.error.issues.map((i) => i.message);
        expect(errorMessages).toContain('Matches pattern is required');
        expect(errorMessages.some((msg) => msg.includes('Invalid enum value'))).toBe(true);
      }
    });

    it('should not throw errors', () => {
      const testCases = [validRule, {}, null, undefined, 'string', 123, [], { random: 'object' }];

      testCases.forEach((testCase) => {
        expect(() => isValidPlacementRule(testCase)).not.toThrow();
      });
    });
  });

  describe('Edge cases', () => {
    it('should handle rules with only required fields', () => {
      const minimalRule = {
        type: 'application' as const,
        matches: 'spark*',
        policy: 'user' as const,
      };

      expect(isPlacementRule(minimalRule)).toBe(true);
      expect(validatePlacementRule(minimalRule)).toEqual(minimalRule);
      const result = isValidPlacementRule(minimalRule);
      expect(result.valid).toBe(true);
    });

    it('should handle complex match patterns', () => {
      const complexPatterns = [
        '*',
        'user*',
        '*admin',
        'test-user-123',
        'group.subgroup',
        'app_name',
        'user@domain.com',
      ];

      complexPatterns.forEach((pattern) => {
        const rule = { ...validRule, matches: pattern };
        expect(isPlacementRule(rule)).toBe(true);
      });
    });

    it('should validate custom placement policy correctly', () => {
      const customRule: PlacementRule = {
        type: 'user',
        matches: '*',
        policy: 'custom',
        customPlacement: 'root.custom.%user',
      };

      expect(isPlacementRule(customRule)).toBe(true);
      expect(validatePlacementRule(customRule)).toEqual(customRule);
    });
  });
});
