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


export const CROSS_QUEUE_RULES = [
  'child-capacity-sum',
  'capacity-type-consistency',
  'parent-child-capacity-constraint',
  'parent-child-capacity-mode',
] as const;

export const QUEUE_SPECIFIC_RULES = [
  'max-capacity-minimum',
  'max-capacity-format-match',
  'weight-mode-transition-flexible-aqc',
] as const;

export const WARNING_ONLY_RULES = ['parent-child-capacity-constraint'] as const;

type CrossQueueRule = (typeof CROSS_QUEUE_RULES)[number];
type QueueSpecificRule = (typeof QUEUE_SPECIFIC_RULES)[number];

export function isCrossQueueRule(rule: string): boolean {
  return CROSS_QUEUE_RULES.includes(rule as CrossQueueRule);
}

export function isQueueSpecificRule(rule: string): boolean {
  return QUEUE_SPECIFIC_RULES.includes(rule as QueueSpecificRule);
}

export function isBlockingError(rule: string, severity: 'error' | 'warning'): boolean {
  if (severity === 'warning') {
    return false;
  }

  if (isCrossQueueRule(rule)) {
    return false;
  }

  return true;
}
