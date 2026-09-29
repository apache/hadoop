import { describe, it, expect, beforeEach } from 'vitest';
import { createQueueSelectionSlice } from '../queueSelectionSlice';
import type { QueueSelectionSlice, SchedulerStore } from '../types';

describe('QueueSelectionSlice - Comparison Mode', () => {
  let slice: QueueSelectionSlice;
  let mockGet: () => Partial<SchedulerStore>;
  let mockSet: (fn: (state: Partial<SchedulerStore>) => void) => void;
  let state: Partial<SchedulerStore>;

  beforeEach(() => {
    state = {
      comparisonQueues: [],
      isComparisonModeActive: false,
    };

    mockGet = () => state;
    mockSet = (fn) => {
      fn(state);
    };

    slice = createQueueSelectionSlice(mockSet as any, mockGet as any, {} as any);
  });

  describe('toggleComparisonMode', () => {
    it('should toggle comparison mode from false to true', () => {
      expect(state.isComparisonModeActive).toBe(false);

      slice.toggleComparisonMode();

      expect(state.isComparisonModeActive).toBe(true);
    });

    it('should toggle comparison mode from true to false', () => {
      state.isComparisonModeActive = true;

      slice.toggleComparisonMode();

      expect(state.isComparisonModeActive).toBe(false);
    });

    it('should clear comparison queues when toggling off', () => {
      state.isComparisonModeActive = true;
      state.comparisonQueues = ['root.default', 'root.production'];

      slice.toggleComparisonMode();

      expect(state.isComparisonModeActive).toBe(false);
      expect(state.comparisonQueues).toEqual([]);
    });

    it('should not clear comparison queues when toggling on', () => {
      state.isComparisonModeActive = false;
      state.comparisonQueues = ['root.default'];

      slice.toggleComparisonMode();

      expect(state.isComparisonModeActive).toBe(true);
      expect(state.comparisonQueues).toEqual(['root.default']);
    });
  });

  describe('setComparisonMode', () => {
    it('should set comparison mode to true', () => {
      slice.setComparisonMode(true);

      expect(state.isComparisonModeActive).toBe(true);
    });

    it('should set comparison mode to false', () => {
      state.isComparisonModeActive = true;

      slice.setComparisonMode(false);

      expect(state.isComparisonModeActive).toBe(false);
    });

    it('should clear comparison queues when setting to false', () => {
      state.isComparisonModeActive = true;
      state.comparisonQueues = ['root.default', 'root.production', 'root.dev'];

      slice.setComparisonMode(false);

      expect(state.isComparisonModeActive).toBe(false);
      expect(state.comparisonQueues).toEqual([]);
    });

    it('should not clear comparison queues when setting to true', () => {
      state.isComparisonModeActive = false;
      state.comparisonQueues = ['root.default', 'root.production'];

      slice.setComparisonMode(true);

      expect(state.isComparisonModeActive).toBe(true);
      expect(state.comparisonQueues).toEqual(['root.default', 'root.production']);
    });
  });
});
