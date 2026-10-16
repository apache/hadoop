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


import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { SearchBar } from './SearchBar';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { useDebounce } from '~/hooks/useDebounce';

// Mock dependencies
vi.mock('~/stores/schedulerStore');
vi.mock('~/hooks/useDebounce');

describe('SearchBar', () => {
  const mockSetSearchQuery = vi.fn();
  const mockClearSearch = vi.fn();
  const mockSetSearchFocused = vi.fn();
  const mockGetFilteredQueues = vi.fn();
  const mockGetFilteredNodes = vi.fn();
  const mockGetFilteredSettings = vi.fn();

  const defaultStoreState = {
    searchQuery: '',
    searchContext: 'queues' as const,
    isSearchFocused: false,
    setSearchQuery: mockSetSearchQuery,
    clearSearch: mockClearSearch,
    setSearchFocused: mockSetSearchFocused,
    getFilteredQueues: mockGetFilteredQueues,
    getFilteredNodes: mockGetFilteredNodes,
    getFilteredSettings: mockGetFilteredSettings,
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...defaultStoreState, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockStore();
    vi.mocked(useDebounce).mockImplementation((value) => value);
    mockGetFilteredQueues.mockReturnValue(null);
    mockGetFilteredNodes.mockReturnValue([]);
    mockGetFilteredSettings.mockReturnValue([]);
  });

  describe('Rendering', () => {
    it('should render with default placeholder', () => {
      render(<SearchBar />);
      expect(screen.getByLabelText('Search Queues')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('Search... in Queues')).toBeInTheDocument();
    });

    it('should render with custom placeholder', () => {
      render(<SearchBar placeholder="Find items" />);
      expect(screen.getByPlaceholderText('Find items in Queues')).toBeInTheDocument();
    });

    it('should display correct context label for different contexts', () => {
      // Test initial context
      render(<SearchBar />);
      expect(screen.getByPlaceholderText('Search... in Queues')).toBeInTheDocument();

      // Clean up
      vi.clearAllMocks();

      // Test nodes context
      mockStore({
        searchContext: 'nodes',
        getFilteredQueues: mockGetFilteredQueues,
        getFilteredNodes: mockGetFilteredNodes,
        getFilteredSettings: mockGetFilteredSettings,
      });
      const { unmount: unmount1 } = render(<SearchBar />);
      expect(screen.getByPlaceholderText('Search... in Nodes')).toBeInTheDocument();
      unmount1();

      // Test settings context
      mockStore({
        searchContext: 'settings',
        getFilteredQueues: mockGetFilteredQueues,
        getFilteredNodes: mockGetFilteredNodes,
        getFilteredSettings: mockGetFilteredSettings,
      });
      render(<SearchBar />);
      expect(screen.getByPlaceholderText('Search... in Settings')).toBeInTheDocument();
    });

    it('should not display clear button when search is empty', () => {
      render(<SearchBar />);
      expect(screen.queryByLabelText('Clear search')).not.toBeInTheDocument();
    });

    it('should display clear button and match count when search has value', () => {
      // Mock the store to have an active search with results
      // Use settings context to match the filtered settings we're providing
      mockStore({
        searchQuery: 'test',
        searchContext: 'settings',
        getFilteredQueues: () => null,
        getFilteredNodes: () => [],
        getFilteredSettings: () => [{}, {}, {}, {}, {}], // 5 items
      });

      render(<SearchBar />);

      // When there's a search query, should show clear button and match count
      expect(screen.getByLabelText('Clear search')).toBeInTheDocument();
      expect(screen.getByText('5 matches')).toBeInTheDocument();
      // Input should reflect the query from the store
      expect(screen.getByLabelText('Search Settings')).toHaveValue('test');
    });

    it('should display singular match for count of 1', () => {
      // Mock the store to have an active search with 1 result
      // Use settings context to match the filtered settings we're providing
      mockStore({
        searchQuery: 'test',
        searchContext: 'settings',
        getFilteredQueues: () => null,
        getFilteredNodes: () => [],
        getFilteredSettings: () => [{}], // 1 item
      });

      render(<SearchBar />);

      expect(screen.getByText('1 match')).toBeInTheDocument();
    });

    it('should apply custom className', () => {
      render(<SearchBar className="custom-class" />);
      const searchBar = screen.getByLabelText('Search Queues').closest('.relative');
      expect(searchBar?.parentElement).toHaveClass('custom-class');
    });
  });

  describe('User Interactions', () => {
    it('should update local state when typing', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues') as HTMLInputElement;

      await user.type(input, 'hello');

      expect(input.value).toBe('hello');
    });

    it('should call setSearchQuery with debounced value', async () => {
      const mockDebouncedValue = 'debounced-test';
      vi.mocked(useDebounce).mockReturnValue(mockDebouncedValue);

      const { rerender } = render(<SearchBar />);

      // Trigger effect by causing a re-render
      rerender(<SearchBar />);

      await waitFor(() => {
        expect(mockSetSearchQuery).toHaveBeenCalledWith(mockDebouncedValue);
      });
    });

    it('should clear search when clear button is clicked', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues') as HTMLInputElement;

      await user.type(input, 'test');
      const clearButton = screen.getByLabelText('Clear search');

      await user.click(clearButton);

      expect(input.value).toBe('');
      expect(mockClearSearch).toHaveBeenCalled();
      expect(input).toHaveFocus();
    });

    it('should set search focused state on focus and blur', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues');

      await user.click(input);
      expect(mockSetSearchFocused).toHaveBeenCalledWith(true);

      await user.tab(); // Blur
      expect(mockSetSearchFocused).toHaveBeenCalledWith(false);
    });
  });

  describe('Keyboard Shortcuts', () => {
    it('should focus input on Cmd+G', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues');

      await user.keyboard('{Meta>}g{/Meta}');
      expect(input).toHaveFocus();
    });

    it('should focus input on Ctrl+G', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues');

      await user.keyboard('{Control>}g{/Control}');
      expect(input).toHaveFocus();
    });

    it('should clear search on Escape when focused', async () => {
      const user = userEvent.setup();
      mockStore({
        isSearchFocused: true,
        getFilteredQueues: mockGetFilteredQueues,
        getFilteredNodes: mockGetFilteredNodes,
        getFilteredSettings: mockGetFilteredSettings,
      });

      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues') as HTMLInputElement;

      await user.type(input, 'test');
      await user.keyboard('{Escape}');

      expect(input.value).toBe('');
      expect(mockClearSearch).toHaveBeenCalled();
      expect(input).not.toHaveFocus();
    });

    it('should not clear search on Escape when not focused', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues') as HTMLInputElement;

      await user.type(input, 'test');
      await user.tab(); // Blur
      await user.keyboard('{Escape}');

      expect(input.value).toBe('test');
      expect(mockClearSearch).not.toHaveBeenCalled();
    });

    it('should display keyboard shortcuts hint when focused', () => {
      mockStore({
        isSearchFocused: true,
        getFilteredQueues: mockGetFilteredQueues,
        getFilteredNodes: mockGetFilteredNodes,
        getFilteredSettings: mockGetFilteredSettings,
      });

      render(<SearchBar />);

      expect(screen.getByText(/⌘G/)).toBeInTheDocument();
      expect(screen.getByText(/focus/)).toBeInTheDocument();
      expect(screen.getByText(/Esc/)).toBeInTheDocument();
      expect(screen.getByText(/clear/)).toBeInTheDocument();
    });

    it('should not display keyboard shortcuts hint when not focused', () => {
      render(<SearchBar />);

      expect(screen.queryByText(/⌘G/)).not.toBeInTheDocument();
      expect(screen.queryByText(/focus/)).not.toBeInTheDocument();
    });
  });

  describe('Debouncing', () => {
    it('should debounce search query updates', async () => {
      // Mock useDebounce to pass through the value immediately for testing
      vi.mocked(useDebounce).mockImplementation((value) => value);

      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues');

      const user = userEvent.setup();
      await user.type(input, 'test');

      // Since we mocked debounce to pass through, it should be called with 'test'
      await waitFor(() => {
        expect(mockSetSearchQuery).toHaveBeenLastCalledWith('test');
      });

      // Verify the debounce was called with correct delay
      expect(useDebounce).toHaveBeenCalledWith('test', 300);
    });
  });

  describe('Accessibility', () => {
    it('should have proper ARIA labels', () => {
      render(<SearchBar />);

      const input = screen.getByLabelText('Search Queues');
      expect(input).toHaveAttribute('aria-label', 'Search Queues');
    });

    it('should have proper ARIA label for clear button', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);

      await user.type(screen.getByLabelText('Search Queues'), 'test');

      const clearButton = screen.getByLabelText('Clear search');
      expect(clearButton).toHaveAttribute('aria-label', 'Clear search');
    });

    it('should be keyboard navigable', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);

      await user.tab();
      expect(screen.getByLabelText('Search Queues')).toHaveFocus();

      await user.type(screen.getByLabelText('Search Queues'), 'test');
      await user.tab();
      expect(screen.getByLabelText('Clear search')).toHaveFocus();
    });
  });

  describe('Edge Cases', () => {
    it('should handle rapid typing and clearing', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues') as HTMLInputElement;

      await user.type(input, 'test');
      await user.clear(input);
      await user.type(input, 'new');
      await user.clear(input);

      expect(input.value).toBe('');
    });

    it('should maintain focus after clearing with button', async () => {
      const user = userEvent.setup();
      render(<SearchBar />);
      const input = screen.getByLabelText('Search Queues');

      await user.type(input, 'test');
      await user.click(screen.getByLabelText('Clear search'));

      expect(input).toHaveFocus();
    });

    it('should handle empty search results gracefully', async () => {
      const user = userEvent.setup();
      // All filtered results return empty arrays (0 matches)
      mockGetFilteredQueues.mockReturnValue(null);
      mockGetFilteredNodes.mockReturnValue([]);
      mockGetFilteredSettings.mockReturnValue([]);

      render(<SearchBar />);
      await user.type(screen.getByLabelText('Search Queues'), 'test');

      expect(screen.getByText('0 matches')).toBeInTheDocument();
    });

    it('should cleanup event listeners on unmount', () => {
      const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');
      const { unmount } = render(<SearchBar />);

      unmount();

      expect(removeEventListenerSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
      removeEventListenerSpy.mockRestore();
    });
  });
});
