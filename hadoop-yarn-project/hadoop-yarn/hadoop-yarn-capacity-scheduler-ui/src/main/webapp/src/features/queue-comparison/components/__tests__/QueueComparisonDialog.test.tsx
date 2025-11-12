import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueueComparisonDialog } from '../QueueComparisonDialog';
import { useSchedulerStore } from '~/stores/schedulerStore';

vi.mock('~/stores/schedulerStore');
vi.mock('../ComparisonTable', () => ({
  ComparisonTable: ({ data }: any) => (
    <div data-testid="comparison-table">
      <div data-testid="queue-count">{data.queues.length}</div>
    </div>
  ),
}));

describe('QueueComparisonDialog', () => {
  const mockOnOpenChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should show correct queue count when comparing 2 queues', () => {
    const mockConfigData = new Map<string, string>([
      ['yarn.scheduler.capacity.root.default.capacity', '50'],
      ['yarn.scheduler.capacity.root.production.capacity', '50'],
    ]);

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: ['root.default', 'root.production'],
        configData: mockConfigData,
      };
      return selector ? selector(state) : state;
    });

    render(<QueueComparisonDialog open={true} onOpenChange={mockOnOpenChange} />);

    expect(screen.getByText(/Comparing 2 queues/i)).toBeInTheDocument();
    expect(screen.getByTestId('queue-count')).toHaveTextContent('2');
  });

  it('should show correct queue count when comparing 3 queues', () => {
    const mockConfigData = new Map<string, string>([
      ['yarn.scheduler.capacity.root.default.capacity', '30'],
      ['yarn.scheduler.capacity.root.production.capacity', '40'],
      ['yarn.scheduler.capacity.root.dev.capacity', '30'],
    ]);

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: ['root.default', 'root.production', 'root.dev'],
        configData: mockConfigData,
      };
      return selector ? selector(state) : state;
    });

    render(<QueueComparisonDialog open={true} onOpenChange={mockOnOpenChange} />);

    expect(screen.getByText(/Comparing 3 queues/i)).toBeInTheDocument();
    expect(screen.getByTestId('queue-count')).toHaveTextContent('3');
  });

  it('should show 0 queues when comparison array is empty', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        configData: new Map(),
      };
      return selector ? selector(state) : state;
    });

    render(<QueueComparisonDialog open={true} onOpenChange={mockOnOpenChange} />);

    expect(screen.getByText(/Comparing 0 queues/i)).toBeInTheDocument();
    expect(screen.getByTestId('queue-count')).toHaveTextContent('0');
  });

  it('should reactively update when comparison queues change', () => {
    const mockConfigData = new Map<string, string>([
      ['yarn.scheduler.capacity.root.default.capacity', '50'],
      ['yarn.scheduler.capacity.root.production.capacity', '50'],
    ]);

    let currentQueues = ['root.default', 'root.production'];

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: currentQueues,
        configData: mockConfigData,
      };
      return selector ? selector(state) : state;
    });

    const { rerender } = render(
      <QueueComparisonDialog open={true} onOpenChange={mockOnOpenChange} />,
    );

    expect(screen.getByText(/Comparing 2 queues/i)).toBeInTheDocument();

    // Simulate queue selection change
    currentQueues = ['root.default', 'root.production', 'root.dev'];

    rerender(<QueueComparisonDialog open={true} onOpenChange={mockOnOpenChange} />);

    expect(screen.getByText(/Comparing 3 queues/i)).toBeInTheDocument();
  });

  it('should not render when dialog is closed', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: ['root.default', 'root.production'],
        configData: new Map(),
      };
      return selector ? selector(state) : state;
    });

    const { container } = render(
      <QueueComparisonDialog open={false} onOpenChange={mockOnOpenChange} />,
    );

    // Dialog should not be visible when open=false
    expect(container.querySelector('[role="dialog"]')).not.toBeInTheDocument();
  });
});
