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
import { ERROR_CODES } from '~/lib/errors/error-codes';

describe('ERROR_CODES', () => {
  it('should define all expected error codes', () => {
    const expectedCodes = [
      'LOAD_INITIAL_DATA_FAILED',
      'REFRESH_SCHEDULER_FAILED',
      'APPLY_CHANGES_FAILED',
      'INVALID_QUEUE_PATH',
      'INVALID_PROPERTY_NAME',
      'INVALID_PROPERTY_VALUE',
      'INVALID_QUEUE_NAME',
      'EMPTY_STAGED_CHANGES',
      'API_ERROR',
      'NETWORK_ERROR',
      'VALIDATION_ERROR',
      'ADD_NODE_LABEL_FAILED',
      'REMOVE_NODE_LABEL_FAILED',
      'ASSIGN_NODE_TO_LABEL_FAILED',
      'QUEUE_ALREADY_EXISTS',
      'MUTATION_BLOCKED',
    ];

    const actualCodes = Object.keys(ERROR_CODES);

    expect(actualCodes).toHaveLength(expectedCodes.length);
    expectedCodes.forEach((code) => {
      expect(actualCodes).toContain(code);
    });
  });

  it('should have no duplicate values', () => {
    const values = Object.values(ERROR_CODES);
    const uniqueValues = new Set(values);

    expect(values.length).toBe(uniqueValues.size);
  });

  it('should have SCREAMING_SNAKE_CASE names', () => {
    const screamingSnakeCasePattern = /^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$/;

    Object.keys(ERROR_CODES).forEach((code) => {
      expect(code).toMatch(screamingSnakeCasePattern);
    });
  });

  it('should have values matching their keys', () => {
    Object.entries(ERROR_CODES).forEach(([key, value]) => {
      expect(value).toBe(key);
    });
  });

  it('should maintain type safety with as const', () => {
    // TypeScript will enforce this at compile time, but we can verify the object is frozen-like
    const codes: typeof ERROR_CODES = ERROR_CODES;

    expect(codes.LOAD_INITIAL_DATA_FAILED).toBe('LOAD_INITIAL_DATA_FAILED');
    expect(codes.NETWORK_ERROR).toBe('NETWORK_ERROR');
    expect(codes.MUTATION_BLOCKED).toBe('MUTATION_BLOCKED');
  });

  it('should have specific error codes for data loading', () => {
    expect(ERROR_CODES.LOAD_INITIAL_DATA_FAILED).toBeDefined();
    expect(ERROR_CODES.REFRESH_SCHEDULER_FAILED).toBeDefined();
  });

  it('should have specific error codes for mutations', () => {
    expect(ERROR_CODES.APPLY_CHANGES_FAILED).toBeDefined();
    expect(ERROR_CODES.MUTATION_BLOCKED).toBeDefined();
  });

  it('should have specific error codes for validation', () => {
    expect(ERROR_CODES.INVALID_QUEUE_PATH).toBeDefined();
    expect(ERROR_CODES.INVALID_PROPERTY_NAME).toBeDefined();
    expect(ERROR_CODES.INVALID_PROPERTY_VALUE).toBeDefined();
    expect(ERROR_CODES.INVALID_QUEUE_NAME).toBeDefined();
    expect(ERROR_CODES.VALIDATION_ERROR).toBeDefined();
  });

  it('should have specific error codes for node labels', () => {
    expect(ERROR_CODES.ADD_NODE_LABEL_FAILED).toBeDefined();
    expect(ERROR_CODES.REMOVE_NODE_LABEL_FAILED).toBeDefined();
    expect(ERROR_CODES.ASSIGN_NODE_TO_LABEL_FAILED).toBeDefined();
  });

  it('should have specific error codes for network issues', () => {
    expect(ERROR_CODES.API_ERROR).toBeDefined();
    expect(ERROR_CODES.NETWORK_ERROR).toBeDefined();
  });

  it('should have specific error codes for business logic', () => {
    expect(ERROR_CODES.EMPTY_STAGED_CHANGES).toBeDefined();
    expect(ERROR_CODES.QUEUE_ALREADY_EXISTS).toBeDefined();
  });
});
