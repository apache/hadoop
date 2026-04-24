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


import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlobalRefreshButton } from './GlobalRefreshButton';
import { useSchedulerStore } from '~/stores/schedulerStore';

vi.mock('~/stores/schedulerStore');

describe('GlobalRefreshButton', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls loadInitialData when clicked', async () => {
    const loadInitialData = vi.fn().mockResolvedValue(undefined);
    const state = {
      loadInitialData,
      isLoading: false,
    };

    vi.mocked(useSchedulerStore).mockImplementation((selector: any) => selector(state));

    const user = userEvent.setup();
    render(<GlobalRefreshButton />);

    const button = screen.getByRole('button', { name: /refresh data/i });
    await user.click(button);

    expect(loadInitialData).toHaveBeenCalledTimes(1);
  });

  it('disables the button and shows spinner while loading', () => {
    const loadInitialData = vi.fn().mockResolvedValue(undefined);
    const state = {
      loadInitialData,
      isLoading: true,
    };

    vi.mocked(useSchedulerStore).mockImplementation((selector: any) => selector(state));

    render(<GlobalRefreshButton />);

    const button = screen.getByRole('button', { name: /refresh data/i });
    expect(button).toBeDisabled();

    const icon = button.querySelector('svg');
    expect(icon).toHaveClass('animate-spin');
  });
});
