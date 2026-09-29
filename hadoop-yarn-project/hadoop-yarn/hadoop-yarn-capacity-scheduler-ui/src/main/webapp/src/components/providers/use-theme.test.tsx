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


import { renderHook, act } from '@testing-library/react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { useTheme } from './use-theme';
import { ThemeProvider } from './theme-provider';

const matchMediaMock = () => ({
  matches: false,
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  addListener: vi.fn(),
  removeListener: vi.fn(),
  dispatchEvent: vi.fn(),
  onchange: null,
  media: '(prefers-color-scheme: dark)',
});

describe('useTheme', () => {
  beforeAll(() => {
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation(matchMediaMock),
    });
  });

  beforeEach(() => {
    document.documentElement.className = '';
    window.localStorage.clear();
  });

  it('throws when used outside ThemeProvider', () => {
    expect(() => renderHook(() => useTheme())).toThrowError(
      'useTheme must be used within a ThemeProvider',
    );
  });

  it('provides theme value and setter inside ThemeProvider', () => {
    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider storageKey="test-theme">{children}</ThemeProvider>,
    });

    expect(result.current.theme).toBe('system');

    act(() => {
      result.current.setTheme('dark');
    });

    expect(result.current.theme).toBe('dark');
    expect(window.localStorage.getItem('test-theme')).toBe('dark');
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });
});
