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


/**
 * Node label validation utilities according to YARN Capacity Scheduler specification
 *
 * - Pattern: ^[0-9a-zA-Z][0-9a-zA-Z-_]*$
 * - Maximum length: 255 characters
 * - Cannot use "DEFAULT" as label name (reserved)
 * - Must be unique in cluster
 */

export type ValidationResult = {
  valid: boolean;
  error?: string;
};

/**
 * Validates a node label name according to YARN specification
 * @param labelName The label name to validate
 * @param existingLabels Optional array of existing label names to check for duplicates
 * @returns Validation result with error message if invalid
 */
export function validateLabelName(labelName: string, existingLabels?: string[]): ValidationResult {
  const trimmedName = labelName.trim();

  // Empty name check
  if (!trimmedName) {
    return { valid: false, error: 'Label name is required' };
  }

  // YARN pattern validation: must start with alphanumeric, then alphanumeric, hyphen, or underscore
  const YARN_LABEL_PATTERN = /^[0-9a-zA-Z][0-9a-zA-Z-_]*$/;
  if (!YARN_LABEL_PATTERN.test(trimmedName)) {
    return {
      valid: false,
      error:
        'Label name must start with a letter or number, and can only contain letters, numbers, hyphens, and underscores',
    };
  }

  // Maximum length validation (YARN limit)
  const MAX_LABEL_LENGTH = 255;
  if (trimmedName.length > MAX_LABEL_LENGTH) {
    return {
      valid: false,
      error: `Label name cannot exceed ${MAX_LABEL_LENGTH} characters`,
    };
  }

  // Reserved name validation
  if (trimmedName.toUpperCase() === 'DEFAULT') {
    return {
      valid: false,
      error: 'Cannot use "DEFAULT" as a label name (reserved)',
    };
  }

  // Duplicate name validation
  if (existingLabels && existingLabels.includes(trimmedName)) {
    return {
      valid: false,
      error: 'Label already exists',
    };
  }

  return { valid: true };
}

/**
 * Validates if a label name is safe to remove
 * @param labelName The label name to check
 * @param nodeAssignments Map of nodeId -> assigned labels to check if label is in use
 * @param queueConfigurations Map of queuePath -> accessible labels to check if queues use this label
 * @returns Validation result with error if label cannot be removed
 */
export function validateLabelRemoval(
  labelName: string,
  nodeAssignments: Map<string, string[]>,
  queueConfigurations?: Map<string, string[]>,
): ValidationResult {
  // Check if any nodes are assigned to this label
  const nodesWithLabel = Array.from(nodeAssignments.entries())
    .filter(([_, labels]) => labels.includes(labelName))
    .map(([nodeId]) => nodeId);

  if (nodesWithLabel.length > 0) {
    return {
      valid: false,
      error: `Cannot remove label "${labelName}": ${nodesWithLabel.length} node(s) are assigned to this label. Reassign nodes first.`,
    };
  }

  // Check if any queues are configured to use this label
  if (queueConfigurations) {
    const queuesWithLabel = Array.from(queueConfigurations.entries())
      .filter(([_, labels]) => labels.includes(labelName))
      .map(([queuePath]) => queuePath);

    if (queuesWithLabel.length > 0) {
      return {
        valid: false,
        error: `Cannot remove label "${labelName}": ${queuesWithLabel.length} queue(s) are configured to use this label. Update queue configurations first.`,
      };
    }
  }

  return { valid: true };
}
