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


/**
 * Search bar component with context-aware search functionality
 */

import { useEffect, useRef, useState, useMemo } from 'react';
import { Search, X } from 'lucide-react';
import { Input } from '~/components/ui/input';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { cn } from '~/utils/cn';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { useDebounce } from '~/hooks/useDebounce';
import { calculateSearchResults } from '~/utils/searchUtils';

interface SearchBarProps {
  placeholder?: string;
  className?: string;
}

export const SearchBar: React.FC<SearchBarProps> = ({ placeholder = 'Search...', className }) => {
  const {
    searchQuery,
    searchContext,
    isSearchFocused,
    setSearchQuery,
    clearSearch,
    setSearchFocused,
    getFilteredQueues,
    getFilteredNodes,
    getFilteredSettings,
  } = useSchedulerStore();

  const inputRef = useRef<HTMLInputElement>(null);
  const [localQuery, setLocalQuery] = useState(searchQuery);
  const [isSearching, setIsSearching] = useState(false);

  // Debounce search updates
  const debouncedQuery = useDebounce(localQuery, 300);

  // Update store when debounced value changes
  useEffect(() => {
    setSearchQuery(debouncedQuery);
    setIsSearching(false);
  }, [debouncedQuery, setSearchQuery]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Cmd/Ctrl + G to focus search
      if ((e.metaKey || e.ctrlKey) && e.key === 'g') {
        e.preventDefault();
        inputRef.current?.focus();
      }

      // Escape to clear when focused
      if (e.key === 'Escape' && isSearchFocused) {
        clearSearch();
        setLocalQuery('');
        inputRef.current?.blur();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isSearchFocused, clearSearch]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setLocalQuery(value);
    if (value && value !== debouncedQuery) {
      setIsSearching(true);
    }
  };

  const handleClear = () => {
    setLocalQuery('');
    clearSearch();
    setIsSearching(false);
    inputRef.current?.focus();
  };

  // Calculate search results directly, memoized to avoid unnecessary recalculations
  const results = useMemo(
    () =>
      calculateSearchResults({
        searchQuery,
        searchContext,
        filteredQueues: getFilteredQueues(),
        filteredNodes: getFilteredNodes(),
        filteredSettings: getFilteredSettings(),
      }),
    [searchQuery, searchContext, getFilteredQueues, getFilteredNodes, getFilteredSettings],
  );

  let contextLabel = 'All';
  if (searchContext === 'queues') contextLabel = 'Queues';
  else if (searchContext === 'nodes') contextLabel = 'Nodes';
  else if (searchContext === 'settings') contextLabel = 'Settings';

  return (
    <div className={cn('relative', className)}>
      <div className="relative">
        <Search
          className={cn(
            'absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground transition-all duration-200',
            isSearching && 'animate-pulse',
          )}
        />
        <Input
          ref={inputRef}
          type="search"
          placeholder={`${placeholder} in ${contextLabel}`}
          value={localQuery}
          onChange={handleChange}
          onFocus={() => setSearchFocused(true)}
          onBlur={() => setSearchFocused(false)}
          className="pl-9 pr-20"
          aria-label={`Search ${contextLabel}`}
        />
        {localQuery && (
          <>
            <Badge
              variant="secondary"
              className="absolute right-10 top-1/2 -translate-y-1/2 text-xs transition-all duration-200"
            >
              {results.count} {results.count === 1 ? 'match' : 'matches'}
            </Badge>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleClear}
              className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7 p-0"
              aria-label="Clear search"
            >
              <X className="h-4 w-4" />
            </Button>
          </>
        )}
      </div>

      {isSearchFocused && (
        <div className="absolute right-0 top-full mt-1 text-xs text-muted-foreground z-50 bg-background p-1 rounded shadow-sm transition-opacity duration-200">
          <kbd className="rounded border px-1">⌘G</kbd> to focus •
          <kbd className="rounded border px-1 ml-1">Esc</kbd> to clear
        </div>
      )}
    </div>
  );
};
