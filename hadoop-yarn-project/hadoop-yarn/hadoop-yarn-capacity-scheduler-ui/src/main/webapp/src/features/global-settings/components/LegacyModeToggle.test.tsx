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
import { render, screen, fireEvent } from '@testing-library/react';
import { LegacyModeToggle } from './LegacyModeToggle';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { SPECIAL_VALUES } from '~/types';

vi.mock('~/stores/schedulerStore');

// Mock the LegacyModeDocumentation component
vi.mock('~/features/queue-management/components/LegacyModeDocumentation', () => ({
  LegacyModeDocumentation: ({ children }: any) => <div>{children}</div>,
}));

// Mock the Dialog component to avoid portal issues in tests
vi.mock('~/components/ui/dialog', () => ({
  Dialog: ({ children }: any) => <div>{children}</div>,
  DialogTrigger: ({ children, asChild }: any) => (asChild ? children : <div>{children}</div>),
  DialogContent: ({ children }: any) => <div>{children}</div>,
  DialogHeader: ({ children }: any) => <div>{children}</div>,
  DialogTitle: ({ children }: any) => <h2>{children}</h2>,
  DialogDescription: ({ children }: any) => <p>{children}</p>,
}));

describe('LegacyModeToggle', () => {
  const mockOnChange = vi.fn();
  const defaultProps = {
    value: 'true',
    isStaged: false,
    onChange: mockOnChange,
    property: {
      name: SPECIAL_VALUES.LEGACY_MODE_PROPERTY,
      displayName: 'Enable Legacy Queue Mode',
      description: 'Test description',
    },
  };

  beforeEach(() => {
    vi.clearAllMocks();
    (useSchedulerStore as any).mockReturnValue({
      schedulerData: {
        queueName: 'root',
        queuePath: 'root',
        queues: { queue: [] },
      },
      configData: new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'true']]),
      stagedChanges: [],
    });
  });

  it('should render the toggle with correct state', () => {
    render(<LegacyModeToggle {...defaultProps} />);

    expect(screen.getByText('Enable Legacy Queue Mode')).toBeInTheDocument();
    expect(screen.getByText('Test description')).toBeInTheDocument();

    const toggle = screen.getByRole('switch');
    expect(toggle).toBeChecked();
  });

  it('should show Modified badge when staged', () => {
    render(<LegacyModeToggle {...defaultProps} isStaged={true} />);

    expect(screen.getByText('Modified')).toBeInTheDocument();
  });

  it('should call onChange when toggled', () => {
    render(<LegacyModeToggle {...defaultProps} />);

    const toggle = screen.getByRole('switch');
    fireEvent.click(toggle);

    expect(mockOnChange).toHaveBeenCalledWith('false');
  });

  it('should show preview button', () => {
    render(<LegacyModeToggle {...defaultProps} />);

    expect(screen.getByText('Preview')).toBeInTheDocument();
  });

  it('should disable preview when no scheduler data', () => {
    (useSchedulerStore as any).mockReturnValue({
      schedulerData: null,
      configData: new Map(),
      stagedChanges: [],
    });

    render(<LegacyModeToggle {...defaultProps} />);

    const previewButton = screen.getByRole('button', { name: /preview/i });
    expect(previewButton).toBeDisabled();
  });
});
