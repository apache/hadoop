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


import { render, screen } from '~/testing/setup/setup';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { ModeToggle } from './mode-toggle';

// Mock the useTheme hook
const mockSetTheme = vi.fn();
const mockUseTheme = vi.fn(() => ({
  theme: 'light',
  setTheme: mockSetTheme,
}));

vi.mock('~/components/providers/use-theme', () => ({
  useTheme: () => mockUseTheme(),
}));

describe('ModeToggle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Display and Accessibility', () => {
    it('should render the toggle button with proper accessibility label', () => {
      render(<ModeToggle />);

      const button = screen.getByRole('button', { name: /toggle theme/i });
      expect(button).toBeInTheDocument();
    });

    it('should display both sun and moon icons', () => {
      render(<ModeToggle />);

      // Both icons should be in the DOM
      const sunIcon = document.querySelector('.lucide-sun');
      const moonIcon = document.querySelector('.lucide-moon');

      expect(sunIcon).toBeInTheDocument();
      expect(moonIcon).toBeInTheDocument();
    });

    it('should show sun icon in light mode with correct classes', () => {
      render(<ModeToggle />);

      const sunIcon = document.querySelector('.lucide-sun');
      const moonIcon = document.querySelector('.lucide-moon');

      // Sun should be visible in light mode
      expect(sunIcon).toHaveClass('rotate-0', 'scale-100');
      // Moon should be hidden in light mode
      expect(moonIcon).toHaveClass('rotate-90', 'scale-0');
    });

    it('should show moon icon in dark mode with correct classes', () => {
      // Re-mock with dark theme
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        setTheme: mockSetTheme,
      });

      render(<ModeToggle />);

      const sunIcon = document.querySelector('.lucide-sun');
      const moonIcon = document.querySelector('.lucide-moon');

      // In dark mode, these classes control visibility through CSS
      expect(sunIcon).toHaveClass('dark:-rotate-90', 'dark:scale-0');
      expect(moonIcon).toHaveClass('dark:rotate-0', 'dark:scale-100');
    });
  });

  describe('Dropdown Menu Functionality', () => {
    it('should not show dropdown menu initially', () => {
      render(<ModeToggle />);

      expect(screen.queryByRole('menu')).not.toBeInTheDocument();
    });

    it('should open dropdown menu when button is clicked', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Check that all menu items are visible
      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();
      expect(screen.getByRole('menuitem', { name: /dark/i })).toBeInTheDocument();
      expect(screen.getByRole('menuitem', { name: /system/i })).toBeInTheDocument();
    });

    it('should close dropdown menu after interaction', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);
      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();

      // Select an item to close the menu
      await user.click(screen.getByRole('menuitem', { name: /light/i }));

      // Menu should be closed after selection
      expect(screen.queryByRole('menuitem', { name: /light/i })).not.toBeInTheDocument();

      // Verify we can open it again
      await user.click(button);
      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();
    });

    it('should close dropdown menu when escape key is pressed', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);
      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();

      // Press escape
      await user.keyboard('{Escape}');

      // Menu should be closed
      expect(screen.queryByRole('menuitem', { name: /light/i })).not.toBeInTheDocument();
    });
  });

  describe('Theme Switching Behavior', () => {
    it('should call setTheme with "light" when Light option is clicked', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Click Light option
      const lightOption = screen.getByRole('menuitem', { name: /light/i });
      await user.click(lightOption);

      expect(mockSetTheme).toHaveBeenCalledWith('light');
      expect(mockSetTheme).toHaveBeenCalledTimes(1);
    });

    it('should call setTheme with "dark" when Dark option is clicked', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Click Dark option
      const darkOption = screen.getByRole('menuitem', { name: /dark/i });
      await user.click(darkOption);

      expect(mockSetTheme).toHaveBeenCalledWith('dark');
      expect(mockSetTheme).toHaveBeenCalledTimes(1);
    });

    it('should call setTheme with "system" when System option is clicked', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Click System option
      const systemOption = screen.getByRole('menuitem', { name: /system/i });
      await user.click(systemOption);

      expect(mockSetTheme).toHaveBeenCalledWith('system');
      expect(mockSetTheme).toHaveBeenCalledTimes(1);
    });

    it('should close menu after selecting a theme option', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Click Light option
      const lightOption = screen.getByRole('menuitem', { name: /light/i });
      await user.click(lightOption);

      // Menu should be closed
      expect(screen.queryByRole('menuitem', { name: /light/i })).not.toBeInTheDocument();
    });
  });

  describe('Integration with ThemeProvider', () => {
    it('should use the useTheme hook from theme provider', () => {
      render(<ModeToggle />);

      // The component renders successfully, which means useTheme hook is being used
      expect(screen.getByRole('button', { name: /toggle theme/i })).toBeInTheDocument();
    });

    it('should work with different initial theme values', () => {
      // Test with system theme
      mockUseTheme.mockReturnValue({
        theme: 'system',
        setTheme: mockSetTheme,
      });

      const { rerender } = render(<ModeToggle />);
      expect(screen.getByRole('button', { name: /toggle theme/i })).toBeInTheDocument();

      // Test with dark theme
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        setTheme: mockSetTheme,
      });

      rerender(<ModeToggle />);
      expect(screen.getByRole('button', { name: /toggle theme/i })).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('should handle multiple theme changes in sequence', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      const button = screen.getByRole('button', { name: /toggle theme/i });

      // First interaction - set to dark
      await user.click(button);
      await user.click(screen.getByRole('menuitem', { name: /dark/i }));
      expect(mockSetTheme).toHaveBeenCalledWith('dark');

      // Second interaction - set to system
      await user.click(button);
      await user.click(screen.getByRole('menuitem', { name: /system/i }));
      expect(mockSetTheme).toHaveBeenCalledWith('system');

      // Third interaction - set to light
      await user.click(button);
      await user.click(screen.getByRole('menuitem', { name: /light/i }));
      expect(mockSetTheme).toHaveBeenCalledWith('light');

      // Verify all calls were made
      expect(mockSetTheme).toHaveBeenCalledTimes(3);
    });

    it('should handle theme changes while menu is open', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu
      const button = screen.getByRole('button', { name: /toggle theme/i });
      await user.click(button);

      // Change theme while menu is open
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        setTheme: mockSetTheme,
      });

      // Click dark option
      await user.click(screen.getByRole('menuitem', { name: /dark/i }));

      expect(mockSetTheme).toHaveBeenCalledWith('dark');
    });

    it('should maintain button focus after closing menu with escape', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      const button = screen.getByRole('button', { name: /toggle theme/i });

      // Focus and open menu
      button.focus();
      await user.click(button);

      // Close with escape
      await user.keyboard('{Escape}');

      // Button should still have focus
      expect(button).toHaveFocus();
    });
  });

  describe('Keyboard Navigation', () => {
    it('should support keyboard navigation in dropdown menu', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      // Open menu with keyboard
      const button = screen.getByRole('button', { name: /toggle theme/i });
      button.focus();
      await user.keyboard('{Enter}');

      // Menu should be open
      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();

      // Navigate with arrow keys
      await user.keyboard('{ArrowDown}');
      expect(screen.getByRole('menuitem', { name: /dark/i })).toHaveFocus();

      await user.keyboard('{ArrowDown}');
      expect(screen.getByRole('menuitem', { name: /system/i })).toHaveFocus();

      // Select with Enter
      await user.keyboard('{Enter}');
      expect(mockSetTheme).toHaveBeenCalledWith('system');
    });

    it('should support space key to open menu', async () => {
      const user = userEvent.setup();
      render(<ModeToggle />);

      const button = screen.getByRole('button', { name: /toggle theme/i });
      button.focus();
      await user.keyboard(' ');

      expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();
    });
  });
});
