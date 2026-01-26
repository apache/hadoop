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


import { CONFIG_PREFIXES } from '~/types';
import type {
  TemplateScope,
  TemplateScopeBuilderOptions,
  TemplateScopeGroup,
} from '~/features/template-config/types';

const LEGACY_SUFFIX = 'leaf-queue-template';
const FLEXIBLE_TEMPLATE_SUFFIX = 'auto-queue-creation-v2.template';
const FLEXIBLE_LEAF_SUFFIX = 'auto-queue-creation-v2.leaf-template';
const FLEXIBLE_PARENT_SUFFIX = 'auto-queue-creation-v2.parent-template';

type ScopeMeta = {
  type: TemplateScope['type'];
  suffix: string;
  group: 'legacy' | 'flexible';
  label: string;
  description: string;
};

const SCOPE_METADATA: ScopeMeta[] = [
  {
    type: 'legacyLeaf',
    suffix: LEGACY_SUFFIX,
    group: 'legacy',
    label: 'Leaf queue template',
    description: 'Values inherited by legacy auto-created leaf queues.',
  },
  {
    type: 'flexibleShared',
    suffix: FLEXIBLE_TEMPLATE_SUFFIX,
    group: 'flexible',
    label: 'Shared template',
    description:
      'Values inherited by flexible auto-created queues at or below this scope (parent and leaf).',
  },
  {
    type: 'flexibleParent',
    suffix: FLEXIBLE_PARENT_SUFFIX,
    group: 'flexible',
    label: 'Parent queue template',
    description: 'Overrides for auto-created parent queues (flexible mode).',
  },
  {
    type: 'flexibleLeaf',
    suffix: FLEXIBLE_LEAF_SUFFIX,
    group: 'flexible',
    label: 'Leaf queue template',
    description: 'Overrides for auto-created leaf queues (flexible mode).',
  },
];

const CONFIG_PREFIX = `${CONFIG_PREFIXES.BASE}.`;

function collectPathsForSuffix(
  baseQueuePath: string,
  suffix: string,
  configKeys: Iterable<string>,
): Set<string> {
  const scopePaths = new Set<string>();

  const exactToken = `.${suffix}`;
  const childToken = `${exactToken}.`;

  for (const key of configKeys) {
    if (!key.startsWith(CONFIG_PREFIX)) {
      continue;
    }

    const remainder = key.slice(CONFIG_PREFIX.length);
    if (!remainder.startsWith(`${baseQueuePath}.`)) {
      continue;
    }

    const suffixIndex = remainder.indexOf(childToken);
    if (suffixIndex !== -1) {
      const candidate = remainder.slice(0, suffixIndex + suffix.length + 1);
      if (isScopeUnderQueue(baseQueuePath, candidate)) {
        scopePaths.add(candidate);
      }
      continue;
    }

    if (remainder.endsWith(exactToken)) {
      scopePaths.add(remainder);
    }
  }

  return scopePaths;
}

function isScopeUnderQueue(baseQueuePath: string, scopeQueuePath: string): boolean {
  if (scopeQueuePath === baseQueuePath) {
    return false;
  }

  if (scopeQueuePath.startsWith(`${baseQueuePath}.`)) {
    return true;
  }

  return false;
}

function ensureDefaultScope(
  scopePaths: Set<string>,
  baseQueuePath: string,
  suffix: string,
  enabled: boolean,
): void {
  if (!enabled) {
    return;
  }
  const defaultScope = `${baseQueuePath}.${suffix}`;
  scopePaths.add(defaultScope);
}

function stagedTemplatePathsForSuffix(
  baseQueuePath: string,
  suffix: string,
  stagedQueuePaths: Iterable<string>,
): Set<string> {
  const staged = new Set<string>();

  for (const path of stagedQueuePaths) {
    if (!path.includes(suffix)) {
      continue;
    }
    const defaultScope = `${baseQueuePath}.${suffix}`;
    if (path === defaultScope || isScopeUnderQueue(baseQueuePath, path)) {
      staged.add(path);
    }
  }

  return staged;
}

export function buildTemplateScopeGroups({
  queuePath,
  configData,
  stagedChanges,
  legacyEnabled,
  flexibleEnabled,
}: TemplateScopeBuilderOptions): TemplateScopeGroup[] {
  const stagedQueuePaths = stagedChanges.map((change) => change.queuePath);

  const scopeMap = new Map<ScopeMeta, Set<string>>();

  SCOPE_METADATA.forEach((meta) => {
    const configPaths = collectPathsForSuffix(queuePath, meta.suffix, configData.keys());
    const stagedPaths = stagedTemplatePathsForSuffix(queuePath, meta.suffix, stagedQueuePaths);

    const combined = new Set<string>([...configPaths, ...stagedPaths]);

    if (meta.group === 'legacy') {
      ensureDefaultScope(combined, queuePath, meta.suffix, legacyEnabled);
    } else if (meta.group === 'flexible') {
      ensureDefaultScope(combined, queuePath, meta.suffix, flexibleEnabled);
    }

    scopeMap.set(meta, combined);
  });

  const groupedScopes: Record<'legacy' | 'flexible', TemplateScope[]> = {
    legacy: [],
    flexible: [],
  };

  for (const meta of SCOPE_METADATA) {
    const scopePaths = scopeMap.get(meta);
    if (!scopePaths || scopePaths.size === 0) {
      continue;
    }

    scopePaths.forEach((scopePath) => {
      groupedScopes[meta.group].push(createTemplateScope(queuePath, scopePath, meta.type));
    });
  }

  const result: TemplateScopeGroup[] = [];

  if (groupedScopes.legacy.length > 0) {
    groupedScopes.legacy.sort((a, b) => a.queuePath.localeCompare(b.queuePath));
    result.push({
      type: 'legacy',
      label: 'Legacy auto-created queues',
      scopes: groupedScopes.legacy,
    });
  }

  if (groupedScopes.flexible.length > 0) {
    groupedScopes.flexible.sort((a, b) => a.queuePath.localeCompare(b.queuePath));
    result.push({
      type: 'flexible',
      label: 'Flexible auto-created queues',
      scopes: groupedScopes.flexible,
    });
  }

  return result;
}

export function getSuffixForTemplateType(type: TemplateScope['type']): string {
  switch (type) {
    case 'legacyLeaf':
      return LEGACY_SUFFIX;
    case 'flexibleShared':
      return FLEXIBLE_TEMPLATE_SUFFIX;
    case 'flexibleParent':
      return FLEXIBLE_PARENT_SUFFIX;
    case 'flexibleLeaf':
      return FLEXIBLE_LEAF_SUFFIX;
  }
}

function getScopeMeta(type: TemplateScope['type']): ScopeMeta {
  const meta = SCOPE_METADATA.find((item) => item.type === type);
  if (!meta) {
    throw new Error(`Unsupported template scope type: ${type}`);
  }
  return meta;
}

export function createTemplateScope(
  baseQueuePath: string,
  queuePath: string,
  type: TemplateScope['type'],
): TemplateScope {
  const meta = getScopeMeta(type);
  const defaultScope = `${baseQueuePath}.${meta.suffix}`;
  const isWildcard = queuePath.includes('*');
  const suffixToken = `.${meta.suffix}`;
  const queuePathWithoutSuffix = queuePath.endsWith(suffixToken)
    ? queuePath.slice(0, -suffixToken.length)
    : queuePath;
  return {
    id: `${type}:${queuePath}`,
    queuePath,
    displayName: meta.label,
    displayQueuePath: isWildcard ? queuePathWithoutSuffix || null : null,
    description: meta.description,
    type,
    isWildcard,
    allowDeletion: queuePath !== defaultScope,
  };
}
