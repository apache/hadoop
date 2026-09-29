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


import { render, screen, renderHook, act, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import { ThemeProvider } from './theme-provider';
import { useTheme } from './use-theme';

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
  length: 0,
  key: vi.fn(),
};
Object.defineProperty(window, 'localStorage', { value: localStorageMock });

// Mock matchMedia
const matchMediaMock = vi.fn();
Object.defineProperty(window, 'matchMedia', {
  value: matchMediaMock,
  writable: true,
});

describe('ThemeProvider', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset document classes
    document.documentElement.className = '';
    // Default matchMedia to light mode
    matchMediaMock.mockReturnValue({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    });
  });

  it('should provide theme context to children', () => {
    const TestComponent = () => {
      const { theme } = useTheme();
      return <div>Current theme: {theme}</div>;
    };

    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>,
    );

    expect(screen.getByText('Current theme: system')).toBeInTheDocument();
  });

  it('should throw error when useTheme is used outside ThemeProvider', () => {
    const TestComponent = () => {
      const { theme } = useTheme();
      return <div>{theme}</div>;
    };

    // Suppress console.error for this test
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    expect(() => render(<TestComponent />)).toThrow('useTheme must be used within a ThemeProvider');

    consoleSpy.mockRestore();
  });

  it('should use default theme when no stored value exists', () => {
    localStorageMock.getItem.mockReturnValue(null);

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider defaultTheme="light">{children}</ThemeProvider>,
    });

    expect(result.current.theme).toBe('light');
  });

  it('should load theme from localStorage on mount', () => {
    localStorageMock.getItem.mockReturnValue('dark');

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider storageKey="test-theme">{children}</ThemeProvider>,
    });

    expect(localStorageMock.getItem).toHaveBeenCalledWith('test-theme');
    expect(result.current.theme).toBe('dark');
  });

  it('should save theme to localStorage when changed', () => {
    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider storageKey="test-theme">{children}</ThemeProvider>,
    });

    act(() => {
      result.current.setTheme('dark');
    });

    expect(localStorageMock.setItem).toHaveBeenCalledWith('test-theme', 'dark');
    expect(result.current.theme).toBe('dark');
  });

  it('should apply correct class to document root for light theme', () => {
    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider>{children}</ThemeProvider>,
    });

    act(() => {
      result.current.setTheme('light');
    });

    expect(document.documentElement.classList.contains('light')).toBe(true);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('should apply correct class to document root for dark theme', () => {
    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider>{children}</ThemeProvider>,
    });

    act(() => {
      result.current.setTheme('dark');
    });

    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });

  it('should apply system theme based on prefers-color-scheme when theme is system', () => {
    // Mock system prefers dark mode
    matchMediaMock.mockReturnValue({
      matches: true, // dark mode
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    });

    renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider defaultTheme="system">{children}</ThemeProvider>,
    });

    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });

  it('should apply light theme when system prefers light mode', async () => {
    // Ensure localStorage is not returning any stored value
    localStorageMock.getItem.mockReturnValue(null);

    // Reset document classes before test
    document.documentElement.className = '';

    // Mock system prefers light mode
    matchMediaMock.mockReturnValue({
      matches: false, // light mode
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    });

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider defaultTheme="system">{children}</ThemeProvider>,
    });

    // Wait for all effects to complete
    await waitFor(() => {
      expect(document.documentElement.classList.contains('light')).toBe(true);
    });

    expect(document.documentElement.classList.contains('dark')).toBe(false);
    expect(result.current.theme).toBe('system');
  });

  it('should handle localStorage errors gracefully when loading', () => {
    localStorageMock.getItem.mockImplementation(() => {
      throw new Error('localStorage not available');
    });

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider defaultTheme="light">{children}</ThemeProvider>,
    });

    // Should fall back to default theme
    expect(result.current.theme).toBe('light');
  });

  it('should handle localStorage errors gracefully when saving', () => {
    localStorageMock.setItem.mockImplementation(() => {
      throw new Error('localStorage not available');
    });

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider>{children}</ThemeProvider>,
    });

    // Should not throw when setting theme
    expect(() => {
      act(() => {
        result.current.setTheme('dark');
      });
    }).not.toThrow();

    // Theme should still be updated in state
    expect(result.current.theme).toBe('dark');
  });

  it('should ignore invalid stored theme values', () => {
    localStorageMock.getItem.mockReturnValue('invalid-theme');

    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider defaultTheme="light">{children}</ThemeProvider>,
    });

    // Should use default theme when stored value is invalid
    expect(result.current.theme).toBe('light');
  });

  it('should update theme classes when theme changes', () => {
    const { result } = renderHook(() => useTheme(), {
      wrapper: ({ children }) => <ThemeProvider>{children}</ThemeProvider>,
    });

    // Start with light theme
    act(() => {
      result.current.setTheme('light');
    });
    expect(document.documentElement.classList.contains('light')).toBe(true);

    // Change to dark theme
    act(() => {
      result.current.setTheme('dark');
    });
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);

    // Change to system theme
    act(() => {
      result.current.setTheme('system');
    });
    // With default mock (light mode preference)
    expect(document.documentElement.classList.contains('light')).toBe(true);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });
});
