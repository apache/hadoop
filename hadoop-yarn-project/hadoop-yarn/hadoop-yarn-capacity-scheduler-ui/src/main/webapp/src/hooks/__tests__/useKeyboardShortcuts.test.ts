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


import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useKeyboardShortcuts, isInputElement, getModifierKey } from '~/hooks/useKeyboardShortcuts';

describe('useKeyboardShortcuts', () => {
  beforeEach(() => {
    // Clear all event listeners before each test
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('isInputElement', () => {
    it('should return true for input elements', () => {
      const input = document.createElement('input');
      expect(isInputElement(input)).toBe(true);
    });

    it('should return true for textarea elements', () => {
      const textarea = document.createElement('textarea');
      expect(isInputElement(textarea)).toBe(true);
    });

    it('should return true for select elements', () => {
      const select = document.createElement('select');
      expect(isInputElement(select)).toBe(true);
    });

    it('should return true for contenteditable elements', () => {
      const div = document.createElement('div');
      div.contentEditable = 'true';
      expect(isInputElement(div)).toBe(true);
    });

    it('should return false for regular div elements', () => {
      const div = document.createElement('div');
      expect(isInputElement(div)).toBe(false);
    });

    it('should return false for button elements', () => {
      const button = document.createElement('button');
      expect(isInputElement(button)).toBe(false);
    });

    it('should return false for null', () => {
      expect(isInputElement(null)).toBe(false);
    });
  });

  describe('getModifierKey', () => {
    it('should return Cmd for macOS', () => {
      // Mock userAgent for macOS
      Object.defineProperty(window.navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        configurable: true,
      });

      expect(getModifierKey()).toBe('Cmd');
    });

    it('should return Ctrl for non-macOS platforms', () => {
      // Mock userAgent for Windows
      Object.defineProperty(window.navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        configurable: true,
      });

      expect(getModifierKey()).toBe('Ctrl');
    });
  });

  describe('useKeyboardShortcuts', () => {
    it('should register keyboard shortcuts', () => {
      const handler = vi.fn();
      const addEventListenerSpy = vi.spyOn(window, 'addEventListener');

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler,
          },
        ]),
      );

      expect(addEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('should call handler when matching shortcut is pressed (Cmd only)', () => {
      const handler = vi.fn();

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler,
          },
        ]),
      );

      // Simulate Cmd+S on macOS (metaKey only)
      const event = new KeyboardEvent('keydown', {
        key: 's',
        metaKey: true,
        ctrlKey: false,
      });
      window.dispatchEvent(event);

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should call handler when matching shortcut is pressed (Ctrl only)', () => {
      const handler = vi.fn();

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler,
          },
        ]),
      );

      // Simulate Ctrl+S on Windows/Linux (ctrlKey only)
      const event = new KeyboardEvent('keydown', {
        key: 's',
        metaKey: false,
        ctrlKey: true,
      });
      window.dispatchEvent(event);

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should call handler with modifier keys even when typing in input field', () => {
      const handler = vi.fn();

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler,
          },
        ]),
      );

      // Create input element and simulate Cmd+S from it
      const input = document.createElement('input');
      document.body.appendChild(input);

      const event = new KeyboardEvent('keydown', {
        key: 's',
        metaKey: true,
        ctrlKey: false,
        bubbles: true,
      });
      Object.defineProperty(event, 'target', { value: input, enumerable: true });

      window.dispatchEvent(event);

      // Should fire because it uses modifiers
      expect(handler).toHaveBeenCalledTimes(1);

      document.body.removeChild(input);
    });

    it('should not call handler without modifiers when typing in input field', () => {
      const handler = vi.fn();

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            preventDefault: true,
            handler,
          },
        ]),
      );

      // Create input element and simulate just pressing 's' (no modifiers)
      const input = document.createElement('input');
      document.body.appendChild(input);

      const event = new KeyboardEvent('keydown', {
        key: 's',
        metaKey: false,
        ctrlKey: false,
        bubbles: true,
      });
      Object.defineProperty(event, 'target', { value: input, enumerable: true });

      window.dispatchEvent(event);

      // Should NOT fire because no modifiers and we're in an input
      expect(handler).not.toHaveBeenCalled();

      document.body.removeChild(input);
    });

    it('should cleanup event listeners on unmount', () => {
      const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');

      const { unmount } = renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: vi.fn(),
          },
        ]),
      );

      unmount();

      expect(removeEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('should handle empty shortcuts array', () => {
      const addEventListenerSpy = vi.spyOn(window, 'addEventListener');

      renderHook(() => useKeyboardShortcuts([]));

      expect(addEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('should only trigger first matching shortcut', () => {
      const handler1 = vi.fn();
      const handler2 = vi.fn();

      renderHook(() =>
        useKeyboardShortcuts([
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: handler1,
          },
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: handler2,
          },
        ]),
      );

      const event = new KeyboardEvent('keydown', {
        key: 's',
        metaKey: true,
        ctrlKey: true,
      });
      window.dispatchEvent(event);

      expect(handler1).toHaveBeenCalledTimes(1);
      expect(handler2).not.toHaveBeenCalled();
    });
  });
});
