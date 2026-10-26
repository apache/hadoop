import { describe, it, expect } from 'vitest';
import { dedupeIssues } from './dedupeIssues';
import type { ValidationIssue } from '~/types';

describe('dedupeIssues', () => {
  it('should return empty array for empty input', () => {
    expect(dedupeIssues([])).toEqual([]);
  });

  it('should return single issue unchanged', () => {
    const issue: ValidationIssue = {
      queuePath: 'root.test',
      field: 'capacity',
      rule: 'test-rule',
      message: 'Test message',
      severity: 'error',
    };
    expect(dedupeIssues([issue])).toEqual([issue]);
  });

  it('should remove exact duplicates', () => {
    const issue: ValidationIssue = {
      queuePath: 'root.test',
      field: 'capacity',
      rule: 'test-rule',
      message: 'Test message',
      severity: 'error',
    };
    expect(dedupeIssues([issue, issue, issue])).toEqual([issue]);
  });

  it('should keep issues with different queuePath', () => {
    const issue1: ValidationIssue = {
      queuePath: 'root.test1',
      field: 'capacity',
      rule: 'test-rule',
      message: 'Test message',
      severity: 'error',
    };
    const issue2: ValidationIssue = {
      ...issue1,
      queuePath: 'root.test2',
    };
    expect(dedupeIssues([issue1, issue2])).toHaveLength(2);
  });

  it('should keep issues with different severity', () => {
    const error: ValidationIssue = {
      queuePath: 'root.test',
      field: 'capacity',
      rule: 'test-rule',
      message: 'Test message',
      severity: 'error',
    };
    const warning: ValidationIssue = {
      ...error,
      severity: 'warning',
    };
    expect(dedupeIssues([error, warning])).toHaveLength(2);
  });

  it('should preserve order of first occurrence', () => {
    const issue1: ValidationIssue = {
      queuePath: 'root.test1',
      field: 'capacity',
      rule: 'rule1',
      message: 'Message 1',
      severity: 'error',
    };
    const issue2: ValidationIssue = {
      queuePath: 'root.test2',
      field: 'capacity',
      rule: 'rule2',
      message: 'Message 2',
      severity: 'warning',
    };
    const result = dedupeIssues([issue1, issue2, issue1, issue2]);
    expect(result).toEqual([issue1, issue2]);
  });
});
