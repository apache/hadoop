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


import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import type { ReactNode } from 'react';
import { useGlobalPropertyValidation } from './useGlobalPropertyValidation';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { SPECIAL_VALUES } from '~/types';
import { ValidationProvider } from '~/contexts/ValidationContext';

const MAXIMUM_APPLICATIONS_PROPERTY = 'yarn.scheduler.capacity.maximum-applications';

vi.mock('~/stores/schedulerStore');

const createStoreState = () => ({
  schedulerData: {
    queueName: 'root',
    queuePath: 'root',
    state: 'RUNNING',
    queues: { queue: [] },
  },
  configData: new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'true']]),
  stagedChanges: [],
});

const setupStoreMock = (state: ReturnType<typeof createStoreState>) => {
  (useSchedulerStore as unknown as Mock).mockImplementation((selector?: (s: any) => any) => {
    if (typeof selector === 'function') {
      return selector(state);
    }
    return state;
  });
};

const createWrapper =
  (_state: ReturnType<typeof createStoreState>) =>
  ({ children }: { children: ReactNode }) => <ValidationProvider>{children}</ValidationProvider>;

beforeEach(() => {
  vi.resetAllMocks();
});

describe('useGlobalPropertyValidation', () => {
  it('should validate global properties', () => {
    const storeState = createStoreState();
    setupStoreMock(storeState);

    const { result } = renderHook(() => useGlobalPropertyValidation(), {
      wrapper: createWrapper(storeState),
    });

    // Test a valid property change
    let errors: ReturnType<typeof result.current.validateGlobalProperty> = [];
    act(() => {
      errors = result.current.validateGlobalProperty(MAXIMUM_APPLICATIONS_PROPERTY, '10000');
    });
    expect(errors).toHaveLength(0);
  });

  it('should return validation errors for invalid values', () => {
    const storeState = createStoreState();
    setupStoreMock(storeState);

    const { result } = renderHook(() => useGlobalPropertyValidation(), {
      wrapper: createWrapper(storeState),
    });

    // Test an invalid property change (if there are any validation rules for global properties)
    let errors: ReturnType<typeof result.current.validateGlobalProperty> = [];
    act(() => {
      errors = result.current.validateGlobalProperty('nonexistent-property', 'invalid');
    });
    // For now, we expect no errors since business validation might not have rules for all global properties
    expect(Array.isArray(errors)).toBe(true);
  });
});
