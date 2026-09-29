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


import { describe, it, expect, vi } from 'vitest';
import { buildComparisonData, exportComparison } from './comparison';
import { getPropertyCategory, formatPropertyName } from '~/utils/formatUtils';

describe('buildComparisonData', () => {
  it('should build comparison data with no differences', () => {
    const configs = new Map([
      ['root.default', { capacity: '50', 'maximum-capacity': '100' }],
      ['root.production', { capacity: '50', 'maximum-capacity': '100' }],
    ]);

    const result = buildComparisonData(configs);

    expect(result.queues).toEqual(['root.default', 'root.production']);
    expect(result.properties.size).toBe(2);
    expect(result.differences.size).toBe(0);
  });

  it('should identify differences between queues', () => {
    const configs = new Map([
      ['root.default', { capacity: '50', 'maximum-capacity': '100' }],
      ['root.production', { capacity: '60', 'maximum-capacity': '100' }],
    ]);

    const result = buildComparisonData(configs);

    expect(result.differences.has('capacity')).toBe(true);
    expect(result.differences.has('maximum-capacity')).toBe(false);
  });

  it('should handle missing properties', () => {
    const configs = new Map<string, Record<string, string>>([
      ['root.default', { capacity: '50', 'user-limit-factor': '1' }],
      ['root.production', { capacity: '50' }],
    ]);

    const result = buildComparisonData(configs);

    expect(result.properties.get('user-limit-factor')?.get('root.default')).toBe('1');
    expect(result.properties.get('user-limit-factor')?.get('root.production')).toBeUndefined();
    expect(result.differences.has('user-limit-factor')).toBe(true);
  });
});

describe('getPropertyCategory', () => {
  it('should categorize node label properties', () => {
    expect(getPropertyCategory('accessible-node-labels')).toBe('Node Labels');
    expect(getPropertyCategory('accessible-node-labels.gpu')).toBe('Node Labels');
  });

  it('should categorize resource properties', () => {
    expect(getPropertyCategory('minimum-user-limit-resource')).toBe('Resources');
    expect(getPropertyCategory('maximum-allocation-resource')).toBe('Resources');
  });

  it('should categorize user limit properties', () => {
    expect(getPropertyCategory('user-limit-factor')).toBe('User Limits');
    expect(getPropertyCategory('minimum-user-limit-percent')).toBe('User Limits');
  });

  it('should categorize application properties', () => {
    expect(getPropertyCategory('maximum-applications')).toBe('Applications');
    expect(getPropertyCategory('maximum-application-lifetime')).toBe('Applications');
  });

  it('should categorize capacity properties', () => {
    expect(getPropertyCategory('capacity')).toBe('Capacity');
    expect(getPropertyCategory('maximum-capacity')).toBe('Capacity');
  });

  it('should default to General for other properties', () => {
    expect(getPropertyCategory('state')).toBe('General');
    expect(getPropertyCategory('priority')).toBe('General');
  });
});

describe('formatPropertyName', () => {
  it('should format hyphenated property names', () => {
    expect(formatPropertyName('maximum-capacity')).toBe('Maximum Capacity');
    expect(formatPropertyName('user-limit-factor')).toBe('User Limit Factor');
  });

  it('should handle single word properties', () => {
    expect(formatPropertyName('capacity')).toBe('Capacity');
    expect(formatPropertyName('state')).toBe('State');
  });
});

describe('exportComparison', () => {
  it('should export comparison data as CSV', () => {
    const mockCreateElement = vi.fn();
    const mockClick = vi.fn();
    const mockCreateObjectURL = vi.fn().mockReturnValue('blob:url');
    const mockRevokeObjectURL = vi.fn();

    global.URL.createObjectURL = mockCreateObjectURL;
    global.URL.revokeObjectURL = mockRevokeObjectURL;

    const anchorElement = {
      href: '',
      download: '',
      click: mockClick,
      style: {},
    };

    // Create a minimal DOM structure
    const mockAppendChild = vi.fn();
    const mockRemoveChild = vi.fn();

    mockCreateElement.mockReturnValue(anchorElement as any);

    // Override document methods
    const originalCreateElement = document.createElement;
    const originalAppendChild = document.body.appendChild;
    const originalRemoveChild = document.body.removeChild;

    document.createElement = mockCreateElement;
    document.body.appendChild = mockAppendChild;
    document.body.removeChild = mockRemoveChild;

    const configs = new Map([
      ['root.default', { capacity: '50' }],
      ['root.production', { capacity: '60' }],
    ]);
    const data = buildComparisonData(configs);

    exportComparison(data);

    expect(mockCreateElement).toHaveBeenCalledWith('a');
    expect(anchorElement.href).toBe('blob:url');
    expect(anchorElement.download).toMatch(/queue-comparison-\d+\.csv/);
    expect(mockClick).toHaveBeenCalled();
    expect(mockAppendChild).toHaveBeenCalledWith(anchorElement);
    expect(mockRemoveChild).toHaveBeenCalledWith(anchorElement);
    expect(mockRevokeObjectURL).toHaveBeenCalledWith('blob:url');

    // Restore original methods
    document.createElement = originalCreateElement;
    document.body.appendChild = originalAppendChild;
    document.body.removeChild = originalRemoveChild;
  });
});
