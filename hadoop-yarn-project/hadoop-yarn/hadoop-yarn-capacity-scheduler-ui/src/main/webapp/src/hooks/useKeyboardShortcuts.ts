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


import { useEffect } from 'react';

/**
 * Keyboard shortcuts utility hook
 * Provides a clean way to register keyboard shortcuts with proper cleanup
 */

type KeyboardShortcutHandler = (event: KeyboardEvent) => void;

interface ShortcutConfig {
  key: string;
  ctrl?: boolean;
  meta?: boolean;
  shift?: boolean;
  alt?: boolean;
  preventDefault?: boolean;
  handler: KeyboardShortcutHandler;
}

/**
 * Checks if the event target is an input element where we should not trigger shortcuts
 */
export function isInputElement(target: EventTarget | null): boolean {
  if (!target || !(target instanceof HTMLElement)) {
    return false;
  }

  const tagName = target.tagName.toLowerCase();
  const isContentEditable = target.isContentEditable;

  // Don't trigger shortcuts when typing in inputs, textareas, or contenteditable elements
  return tagName === 'input' || tagName === 'textarea' || tagName === 'select' || isContentEditable;
}

/**
 * Checks if keyboard event matches the shortcut configuration
 */
function matchesShortcut(event: KeyboardEvent, config: ShortcutConfig): boolean {
  const keyMatch = event.key.toLowerCase() === config.key.toLowerCase();

  // Special handling for cross-platform shortcuts:
  // When both ctrl and meta are specified as true, match if EITHER is pressed
  // This allows Cmd on macOS and Ctrl on Windows/Linux
  let modifierMatch = true;
  if (config.ctrl !== undefined && config.meta !== undefined) {
    if (config.ctrl === true && config.meta === true) {
      // Cross-platform: match if either Ctrl OR Cmd is pressed
      modifierMatch = event.ctrlKey || event.metaKey;
    } else {
      // Both specified but not both true - use exact matching
      const ctrlMatch = event.ctrlKey === config.ctrl;
      const metaMatch = event.metaKey === config.meta;
      modifierMatch = ctrlMatch && metaMatch;
    }
  } else {
    // Only one specified - match exactly
    const ctrlMatch = config.ctrl !== undefined ? event.ctrlKey === config.ctrl : true;
    const metaMatch = config.meta !== undefined ? event.metaKey === config.meta : true;
    modifierMatch = ctrlMatch && metaMatch;
  }

  const shiftMatch = config.shift !== undefined ? event.shiftKey === config.shift : true;
  const altMatch = config.alt !== undefined ? event.altKey === config.alt : true;

  return keyMatch && modifierMatch && shiftMatch && altMatch;
}

/**
 * Hook to register keyboard shortcuts
 *
 * @example
 * ```tsx
 * useKeyboardShortcut({
 *   key: 's',
 *   ctrl: true,  // Ctrl on Windows/Linux
 *   meta: true,  // Cmd on macOS
 *   preventDefault: true,
 *   handler: (e) => handleSave()
 * });
 * ```
 */
export function useKeyboardShortcut(config: ShortcutConfig) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (matchesShortcut(event, config)) {
        // Check if this shortcut uses modifier keys
        const usesModifiers =
          config.ctrl === true ||
          config.meta === true ||
          config.alt === true ||
          config.shift === true;

        // Only block shortcuts in input fields if they DON'T use modifiers
        // Modifier-based shortcuts (Cmd+S, Ctrl+K) should work everywhere
        if (isInputElement(event.target) && !usesModifiers) {
          return;
        }

        if (config.preventDefault) {
          event.preventDefault();
        }
        config.handler(event);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [config]);
}

/**
 * Hook to register multiple keyboard shortcuts at once
 *
 * @example
 * ```tsx
 * useKeyboardShortcuts([
 *   { key: 's', ctrl: true, meta: true, handler: handleSave },
 *   { key: 'r', ctrl: true, meta: true, handler: handleReset }
 * ]);
 * ```
 */
export function useKeyboardShortcuts(configs: ShortcutConfig[]) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      for (const config of configs) {
        if (matchesShortcut(event, config)) {
          // Check if this shortcut uses modifier keys
          const usesModifiers =
            config.ctrl === true ||
            config.meta === true ||
            config.alt === true ||
            config.shift === true;

          // Only block shortcuts in input fields if they DON'T use modifiers
          // Modifier-based shortcuts (Cmd+S, Ctrl+K) should work everywhere
          if (isInputElement(event.target) && !usesModifiers) {
            continue;
          }

          if (config.preventDefault) {
            event.preventDefault();
          }
          config.handler(event);
          break; // Only trigger first matching shortcut
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [configs]);
}

/**
 * Platform-aware modifier key helper
 * Returns the appropriate modifier key based on platform
 */
export function getModifierKey(): 'Ctrl' | 'Cmd' {
  // Check userAgent for macOS
  const isMac = navigator.userAgent.toLowerCase().includes('mac');
  return isMac ? 'Cmd' : 'Ctrl';
}

/**
 * Format shortcut for display
 *
 * @example
 * ```tsx
 * formatShortcut('s', true, true) // Returns "Ctrl+S" or "Cmd+S" based on platform
 * ```
 */
export function formatShortcut(
  key: string,
  ctrl: boolean = false,
  meta: boolean = false,
  shift: boolean = false,
  alt: boolean = false,
): string {
  const parts: string[] = [];

  if (ctrl || meta) {
    parts.push(getModifierKey());
  }
  if (shift) {
    parts.push('Shift');
  }
  if (alt) {
    parts.push('Alt');
  }
  parts.push(key.toUpperCase());

  return parts.join('+');
}
