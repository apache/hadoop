import type { ValidationIssue } from '~/types';

/**
 * Remove duplicate validation issues based on their identity
 * Two issues are considered duplicates if they have the same:
 * - queuePath
 * - field
 * - rule
 * - message
 * - severity
 *
 * @param issues Array of validation issues that may contain duplicates
 * @returns Array of unique validation issues
 */
export function dedupeIssues(issues: ValidationIssue[]): ValidationIssue[] {
  const seen = new Set<string>();
  const result: ValidationIssue[] = [];

  issues.forEach((issue) => {
    const key = `${issue.queuePath}|${issue.field}|${issue.rule}|${issue.message}|${issue.severity}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(issue);
    }
  });

  return result;
}
