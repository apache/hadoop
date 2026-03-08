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


export interface ComparisonData {
  queues: string[];
  properties: Map<string, Map<string, string | undefined>>; // property -> queue -> value
  differences: Set<string>; // properties that differ
}

export const buildComparisonData = (
  configs: Map<string, Record<string, string>>,
): ComparisonData => {
  const queues = Array.from(configs.keys());
  const properties = new Map<string, Map<string, string | undefined>>();
  const differences = new Set<string>();

  // Collect all unique properties
  const allProperties = new Set<string>();
  configs.forEach((config) => {
    Object.keys(config).forEach((prop) => allProperties.add(prop));
  });

  // Build comparison matrix
  allProperties.forEach((prop) => {
    const propValues = new Map<string, string | undefined>();
    let hasDifference = false;
    let firstValue: string | undefined;

    queues.forEach((queue, index) => {
      const value = configs.get(queue)?.[prop];
      propValues.set(queue, value);

      if (index === 0) {
        firstValue = value;
      } else if (value !== firstValue) {
        hasDifference = true;
      }
    });

    properties.set(prop, propValues);
    if (hasDifference) {
      differences.add(prop);
    }
  });

  return { queues, properties, differences };
};

export const exportComparison = (data: ComparisonData) => {
  const { queues, properties } = data;

  // CSV format
  const csv = [
    ['Property', ...queues].join(','),
    ...Array.from(properties.entries()).map(([prop, values]) => {
      const row = [prop];
      queues.forEach((queue) => {
        row.push(values.get(queue) || '');
      });
      return row.map((cell) => `"${cell}"`).join(',');
    }),
  ].join('\n');

  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `queue-comparison-${Date.now()}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};
