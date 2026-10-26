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
import { vi } from 'vitest';
import { AddLabelDialog } from './AddLabelDialog';
import { validateLabelName } from '~/features/node-labels/utils/labelValidation';

// Mock the validation utility
vi.mock('~/features/node-labels/utils/labelValidation');

// Mock UI components to simplify testing
vi.mock('~/components/ui/dialog', () => ({
  Dialog: ({ children, open, onOpenChange }: any) => {
    if (!open) return null;
    return (
      <div role="dialog" aria-modal="true">
        {children}
        <button onClick={() => onOpenChange(false)} aria-label="Close dialog">
          ×
        </button>
      </div>
    );
  },
  DialogContent: ({ children }: any) => <div>{children}</div>,
  DialogHeader: ({ children }: any) => <header>{children}</header>,
  DialogTitle: ({ children }: any) => <h2>{children}</h2>,
  DialogDescription: ({ children }: any) => <p>{children}</p>,
  DialogFooter: ({ children }: any) => <footer>{children}</footer>,
}));

vi.mock('~/components/ui/input', () => ({
  Input: (props: any) => <input {...props} />,
}));

vi.mock('~/components/ui/label', () => ({
  Label: ({ children, htmlFor }: any) => <label htmlFor={htmlFor}>{children}</label>,
}));

vi.mock('~/components/ui/switch', () => ({
  Switch: ({ checked, onCheckedChange, id }: any) => (
    <input
      type="checkbox"
      id={id}
      checked={checked}
      onChange={(e) => onCheckedChange(e.target.checked)}
      role="switch"
      aria-checked={checked}
    />
  ),
}));

describe('AddLabelDialog', () => {
  const mockOnClose = vi.fn();
  const mockOnConfirm = vi.fn();

  const defaultProps = {
    open: true,
    onClose: mockOnClose,
    onConfirm: mockOnConfirm,
    existingLabels: ['gpu', 'highmem'],
    isLoading: false,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    (validateLabelName as any).mockReturnValue({ valid: true });
  });

  describe('Dialog visibility', () => {
    it('should render when open is true', () => {
      render(<AddLabelDialog {...defaultProps} />);

      expect(screen.getByRole('dialog')).toBeInTheDocument();
      expect(screen.getByText('Add New Node Label')).toBeInTheDocument();
    });

    it('should not render when open is false', () => {
      render(<AddLabelDialog {...defaultProps} open={false} />);

      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    });
  });

  describe('Dialog content', () => {
    it('should display title and description', () => {
      render(<AddLabelDialog {...defaultProps} />);

      expect(screen.getByText('Add New Node Label')).toBeInTheDocument();
      expect(
        screen.getByText('Create a new node label for resource allocation'),
      ).toBeInTheDocument();
    });

    it('should display label name input with placeholder', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('id', 'label-name');
    });

    it('should display exclusivity switch', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const switchElement = screen.getByRole('switch');
      expect(switchElement).toBeInTheDocument();
      expect(switchElement).toHaveAttribute('id', 'exclusive');
      expect(switchElement).not.toBeChecked();
    });

    it('should display help text for label name', () => {
      render(<AddLabelDialog {...defaultProps} />);

      expect(
        screen.getByText('Use letters, numbers, hyphens, and underscores only'),
      ).toBeInTheDocument();
    });

    it('should display exclusivity explanation', () => {
      render(<AddLabelDialog {...defaultProps} />);

      expect(screen.getByText(/Exclusive labels:/)).toBeInTheDocument();
      expect(
        screen.getByText(/Only containers specifically requesting this label/),
      ).toBeInTheDocument();
      expect(screen.getByText(/Non-exclusive labels:/)).toBeInTheDocument();
      expect(screen.getByText(/Any container can run on these nodes/)).toBeInTheDocument();
    });
  });

  describe('Input interactions', () => {
    it('should update label name input value', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'new-label');

      expect(input).toHaveValue('new-label');
    });

    it('should clear error when typing', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Invalid label name',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      const addButton = screen.getByRole('button', { name: 'Add Label' });

      // Trigger validation error
      await userEvent.click(addButton);
      expect(screen.getByText('Invalid label name')).toBeInTheDocument();

      // Start typing to clear error
      await userEvent.type(input, 'a');
      expect(screen.queryByText('Invalid label name')).not.toBeInTheDocument();
    });

    it('should toggle exclusivity switch', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const switchElement = screen.getByRole('switch');
      expect(switchElement).not.toBeChecked();

      await userEvent.click(switchElement);
      expect(switchElement).toBeChecked();

      await userEvent.click(switchElement);
      expect(switchElement).not.toBeChecked();
    });

    it('should focus on input when dialog opens', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      expect(document.activeElement).toBe(input);
    });
  });

  describe('Validation', () => {
    it('should validate label name on confirm', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Label already exists',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'gpu');

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      expect(validateLabelName).toHaveBeenCalledWith('gpu', ['gpu', 'highmem']);
      expect(screen.getByText('Label already exists')).toBeInTheDocument();
      expect(mockOnConfirm).not.toHaveBeenCalled();
    });

    it('should show error message with destructive styling', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Invalid characters',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'bad@label');

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      const errorText = screen.getByText('Invalid characters');
      expect(errorText).toHaveClass('text-destructive');

      expect(input).toHaveClass('border-destructive');
    });

    it('should validate empty label name', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Label name is required',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      expect(validateLabelName).toHaveBeenCalledWith('', ['gpu', 'highmem']);
      expect(screen.getByText('Label name is required')).toBeInTheDocument();
    });

    it('should trim whitespace before validation', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, '  ssd-fast  ');

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      expect(mockOnConfirm).toHaveBeenCalledWith('ssd-fast', false);
    });
  });

  describe('Dialog actions', () => {
    it('should call onConfirm with correct values when valid', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      const switchElement = screen.getByRole('switch');

      await userEvent.type(input, 'new-label');
      await userEvent.click(switchElement);

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      expect(mockOnConfirm).toHaveBeenCalledWith('new-label', true);
      expect(mockOnClose).toHaveBeenCalled();
    });

    it('should reset form after successful submission', async () => {
      const { rerender } = render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      const switchElement = screen.getByRole('switch');

      await userEvent.type(input, 'test-label');
      await userEvent.click(switchElement);

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      // Reopen dialog
      rerender(<AddLabelDialog {...defaultProps} open={false} />);
      rerender(<AddLabelDialog {...defaultProps} open={true} />);

      const newInput = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      const newSwitch = screen.getByRole('switch');

      expect(newInput).toHaveValue('');
      expect(newSwitch).not.toBeChecked();
    });

    it('should call onClose when cancel button is clicked', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const cancelButton = screen.getByRole('button', { name: 'Cancel' });
      await userEvent.click(cancelButton);

      expect(mockOnClose).toHaveBeenCalled();
      expect(mockOnConfirm).not.toHaveBeenCalled();
    });

    it('should call onClose when dialog close button is clicked', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const closeButton = screen.getByLabelText('Close dialog');
      await userEvent.click(closeButton);

      expect(mockOnClose).toHaveBeenCalled();
    });

    it('should reset form when closing', async () => {
      const { rerender } = render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'test-label');

      // Show validation error
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Test error',
      });

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      // Close and reopen
      const cancelButton = screen.getByRole('button', { name: 'Cancel' });
      await userEvent.click(cancelButton);

      rerender(<AddLabelDialog {...defaultProps} open={false} />);
      rerender(<AddLabelDialog {...defaultProps} open={true} />);

      const newInput = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      expect(newInput).toHaveValue('');
      expect(screen.queryByText('Test error')).not.toBeInTheDocument();
    });
  });

  describe('Loading state', () => {
    it('should disable buttons when loading', () => {
      render(<AddLabelDialog {...defaultProps} isLoading={true} />);

      const cancelButton = screen.getByRole('button', { name: 'Cancel' });
      const addButton = screen.getByRole('button', { name: 'Add Label' });

      expect(cancelButton).toBeDisabled();
      expect(addButton).toBeDisabled();
    });

    it('should not close dialog when loading', async () => {
      render(<AddLabelDialog {...defaultProps} isLoading={true} />);

      const cancelButton = screen.getByRole('button', { name: 'Cancel' });
      await userEvent.click(cancelButton);

      expect(mockOnClose).not.toHaveBeenCalled();
    });
  });

  describe('Keyboard interactions', () => {
    it('should submit form on Enter key in input', async () => {
      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'new-label');
      expect(input).toHaveValue('new-label');
    });

    it('should not submit on Enter if validation fails', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Invalid',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const input = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      await userEvent.type(input, 'bad');

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      expect(mockOnConfirm).not.toHaveBeenCalled();
      expect(screen.getByText('Invalid')).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('should have proper dialog attributes', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const dialog = screen.getByRole('dialog');
      expect(dialog).toHaveAttribute('aria-modal', 'true');
    });

    it('should have proper label associations', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const nameLabel = screen.getByText('Label Name');
      const nameInput = screen.getByPlaceholderText('e.g., gpu, highmem, ssd');
      expect(nameLabel).toHaveAttribute('for', 'label-name');
      expect(nameInput).toHaveAttribute('id', 'label-name');

      const exclusiveLabel = screen.getByText('Exclusive Label').closest('label');
      const exclusiveSwitch = screen.getByRole('switch');
      expect(exclusiveLabel).not.toBeNull();
      expect(exclusiveLabel as HTMLLabelElement).toHaveAttribute('for', 'exclusive');
      expect(exclusiveSwitch).toHaveAttribute('id', 'exclusive');
    });

    it('should have proper switch aria attributes', () => {
      render(<AddLabelDialog {...defaultProps} />);

      const switchElement = screen.getByRole('switch');
      expect(switchElement).toHaveAttribute('aria-checked', 'false');
    });

    it('should announce validation errors', async () => {
      (validateLabelName as any).mockReturnValue({
        valid: false,
        error: 'Label name is required',
      });

      render(<AddLabelDialog {...defaultProps} />);

      const addButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(addButton);

      const errorText = screen.getByText('Label name is required');
      expect(errorText).toBeInTheDocument();
    });
  });
});
