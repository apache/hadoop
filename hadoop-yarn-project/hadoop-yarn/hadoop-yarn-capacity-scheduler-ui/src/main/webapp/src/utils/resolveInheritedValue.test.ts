import { describe, it, expect, vi } from 'vitest';
import {
  resolveInheritedValue,
  parentChainResolver,
  globalOnlyResolver,
} from './resolveInheritedValue';

function configMap(entries: Record<string, string>): Map<string, string> {
  return new Map(Object.entries(entries));
}

describe('parentChainResolver', () => {
  it('resolves value from direct parent', () => {
    const result = parentChainResolver({
      queuePath: 'root.production.team1',
      propertyName: 'disable_preemption',
      configData: configMap({
        'yarn.scheduler.capacity.root.production.disable_preemption': 'true',
      }),
    });
    expect(result).toEqual({
      value: 'true',
      source: 'queue',
      sourcePath: 'root.production',
    });
  });

  it('resolves value from grandparent when parent is also unset', () => {
    const result = parentChainResolver({
      queuePath: 'root.production.team1',
      propertyName: 'disable_preemption',
      configData: configMap({
        'yarn.scheduler.capacity.root.disable_preemption': 'true',
      }),
    });
    expect(result).toEqual({
      value: 'true',
      source: 'queue',
      sourcePath: 'root',
    });
  });

  it('returns null when no ancestor has the value (no global fallback)', () => {
    const result = parentChainResolver({
      queuePath: 'root.production.team1',
      propertyName: 'disable_preemption',
      configData: configMap({}),
    });
    expect(result).toBeNull();
  });

  it('does NOT fall through to global property with same suffix', () => {
    const result = parentChainResolver({
      queuePath: 'root.production',
      propertyName: 'disable_preemption',
      configData: configMap({
        'yarn.scheduler.capacity.disable_preemption': 'true',
      }),
    });
    expect(result).toBeNull();
  });

  it('still returns inherited source when queue has explicit value (for "overrides" display)', () => {
    const result = parentChainResolver({
      queuePath: 'root.production.team1',
      propertyName: 'disable_preemption',
      configData: configMap({
        'yarn.scheduler.capacity.root.production.team1.disable_preemption': 'false',
        'yarn.scheduler.capacity.root.production.disable_preemption': 'true',
      }),
    });
    expect(result).toEqual({
      value: 'true',
      source: 'queue',
      sourcePath: 'root.production',
    });
  });

  it('returns null for root queue (no parent to walk to)', () => {
    const result = parentChainResolver({
      queuePath: 'root',
      propertyName: 'disable_preemption',
      configData: configMap({}),
    });
    expect(result).toBeNull();
  });
});

describe('globalOnlyResolver', () => {
  it('resolves value from global property (skips parent queues)', () => {
    const result = globalOnlyResolver({
      queuePath: 'root.production.team1',
      propertyName: 'minimum-user-limit-percent',
      configData: configMap({
        'yarn.scheduler.capacity.minimum-user-limit-percent': '100',
      }),
    });
    expect(result).toEqual({
      value: '100',
      source: 'global',
    });
  });

  it('ignores parent queue values', () => {
    const result = globalOnlyResolver({
      queuePath: 'root.production.team1',
      propertyName: 'minimum-user-limit-percent',
      configData: configMap({
        'yarn.scheduler.capacity.root.production.minimum-user-limit-percent': '50',
      }),
    });
    expect(result).toBeNull();
  });

  it('returns null when global is not set', () => {
    const result = globalOnlyResolver({
      queuePath: 'root.production',
      propertyName: 'minimum-user-limit-percent',
      configData: configMap({}),
    });
    expect(result).toBeNull();
  });

  it('still returns global source when queue has explicit value', () => {
    const result = globalOnlyResolver({
      queuePath: 'root.production',
      propertyName: 'minimum-user-limit-percent',
      configData: configMap({
        'yarn.scheduler.capacity.root.production.minimum-user-limit-percent': '50',
        'yarn.scheduler.capacity.minimum-user-limit-percent': '100',
      }),
    });
    expect(result).toEqual({
      value: '100',
      source: 'global',
    });
  });
});

describe('resolveInheritedValue delegation', () => {
  it('returns null when no resolver is provided', () => {
    const result = resolveInheritedValue({
      queuePath: 'root.production',
      propertyName: 'capacity',
      configData: configMap({
        'yarn.scheduler.capacity.root.capacity': '100',
      }),
    });
    expect(result).toBeNull();
  });

  it('delegates to the provided resolver with correct context', () => {
    const resolver = vi.fn().mockReturnValue({ value: '500', source: 'global' as const });
    const data = configMap({ 'yarn.scheduler.capacity.maximum-applications': '10000' });
    const staged = [
      {
        id: '1',
        queuePath: 'root.production',
        property: 'capacity',
        newValue: '50',
        type: 'update' as const,
        timestamp: Date.now(),
      },
    ];

    resolveInheritedValue({
      queuePath: 'root.production',
      propertyName: 'maximum-applications',
      configData: data,
      stagedChanges: staged,
      inheritanceResolver: resolver,
    });

    expect(resolver).toHaveBeenCalledWith({
      queuePath: 'root.production',
      propertyName: 'maximum-applications',
      configData: data,
      stagedChanges: staged,
    });
  });

  it('returns resolver result unchanged', () => {
    const expected = { value: '200', source: 'global' as const, isScaled: true };
    const resolver = vi.fn().mockReturnValue(expected);

    const result = resolveInheritedValue({
      queuePath: 'root.production',
      propertyName: 'maximum-applications',
      configData: configMap({}),
      inheritanceResolver: resolver,
    });

    expect(result).toEqual(expected);
  });

  it('returns null when resolver returns null', () => {
    const resolver = vi.fn().mockReturnValue(null);

    const result = resolveInheritedValue({
      queuePath: 'root.production',
      propertyName: 'maximum-applications',
      configData: configMap({}),
      inheritanceResolver: resolver,
    });

    expect(result).toBeNull();
  });
});

describe('staged changes', () => {
  it('considers parent staged changes via parentChainResolver', () => {
    const result = resolveInheritedValue({
      queuePath: 'root.production.team1',
      propertyName: 'disable_preemption',
      configData: configMap({}),
      inheritanceResolver: parentChainResolver,
      stagedChanges: [
        {
          id: '1',
          queuePath: 'root.production',
          property: 'disable_preemption',
          newValue: 'true',
          type: 'update',
          timestamp: Date.now(),
        },
      ],
    });
    expect(result).toEqual({
      value: 'true',
      source: 'queue',
      sourcePath: 'root.production',
    });
  });

  it('considers global staged changes via globalOnlyResolver', () => {
    const result = resolveInheritedValue({
      queuePath: 'root.production',
      propertyName: 'minimum-user-limit-percent',
      configData: configMap({}),
      inheritanceResolver: globalOnlyResolver,
      stagedChanges: [
        {
          id: '1',
          queuePath: 'global',
          property: 'minimum-user-limit-percent',
          newValue: '75',
          type: 'update',
          timestamp: Date.now(),
        },
      ],
    });
    expect(result).toEqual({
      value: '75',
      source: 'global',
    });
  });
});
