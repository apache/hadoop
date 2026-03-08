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


import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '~/components/ui/card';
import { Button } from '~/components/ui/button';
import { Separator } from '~/components/ui/separator';
import { Badge } from '~/components/ui/badge';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { PlacementRuleForm } from './PlacementRuleForm';
import { getPolicyDescription } from '~/features/placement-rules/constants/policy-descriptions';
import type { PlacementRule } from '~/types/features/placement-rules';

export function PlacementRuleDetail() {
  // State values (trigger re-renders only when these specific values change)
  const { rules, selectedRuleIndex } = useSchedulerStore(
    useShallow((s) => ({
      rules: s.rules,
      selectedRuleIndex: s.selectedRuleIndex,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const updateRule = useSchedulerStore((s) => s.updateRule);
  const [isEditing, setIsEditing] = useState(false);

  if (selectedRuleIndex === null || !rules[selectedRuleIndex]) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center">
          <h3 className="text-lg font-medium text-muted-foreground">No rule selected</h3>
          <p className="text-sm text-muted-foreground mt-2">
            Select a rule from the list to view details
          </p>
        </div>
      </div>
    );
  }

  const rule = rules[selectedRuleIndex];

  const handleUpdate = (data: PlacementRule) => {
    if (selectedRuleIndex !== null) {
      updateRule(selectedRuleIndex, data);
      setIsEditing(false);
    }
  };

  if (isEditing) {
    return (
      <div className="p-6">
        <PlacementRuleForm
          rule={rule}
          ruleIndex={selectedRuleIndex}
          onSubmit={handleUpdate}
          onCancel={() => setIsEditing(false)}
        />
      </div>
    );
  }

  const getPolicyDisplay = (policy: string): string => {
    const policyMap: Record<string, string> = {
      user: 'User Queue',
      primaryGroup: 'Primary Group',
      primaryGroupUser: 'Primary Group → User',
      secondaryGroup: 'Secondary Group',
      secondaryGroupUser: 'Secondary Group → User',
      specified: 'Specified Queue',
      defaultQueue: 'Default Queue',
      setDefaultQueue: 'Set as Default Queue',
      reject: 'Reject Application',
      custom: 'Custom Placement',
    };
    return policyMap[policy] || policy;
  };

  const getRuleTypeColor = (type: string) => {
    switch (type) {
      case 'user':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
      case 'group':
        return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
      case 'application':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200';
    }
  };

  return (
    <Card className="h-full border-0 shadow-none">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-xl">Rule #{selectedRuleIndex + 1}</CardTitle>
            <CardDescription>
              {rule.type} rule matching "{rule.matches}"
            </CardDescription>
          </div>
          <Button onClick={() => setIsEditing(true)}>Edit Rule</Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        <div>
          <h4 className="text-sm font-medium mb-3">Rule Configuration</h4>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Type</span>
              <Badge className={getRuleTypeColor(rule.type)}>{rule.type}</Badge>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Matches</span>
              <span className="font-mono text-sm">{rule.matches}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Policy</span>
              <span className="font-medium">{getPolicyDisplay(rule.policy)}</span>
            </div>
          </div>
        </div>

        <Separator />

        <div>
          <h4 className="text-sm font-medium mb-3">Queue Settings</h4>
          <div className="space-y-3">
            {rule.parentQueue && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Parent Queue</span>
                <span className="font-mono text-sm">{rule.parentQueue}</span>
              </div>
            )}
            {rule.value && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Queue Value</span>
                <span className="font-mono text-sm">{rule.value}</span>
              </div>
            )}
            {rule.customPlacement && (
              <div>
                <span className="text-sm text-muted-foreground">Custom Placement</span>
                <div className="mt-1 p-2 bg-muted rounded-md">
                  <code className="text-xs">{rule.customPlacement}</code>
                </div>
              </div>
            )}
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Create Queue</span>
              <Badge variant={rule.create ? 'default' : 'secondary'}>
                {rule.create ? 'Yes' : 'No'}
              </Badge>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Fallback</span>
              <span className="text-sm font-medium">{rule.fallbackResult || 'skip'}</span>
            </div>
          </div>
        </div>

        <Separator />

        <div>
          <h4 className="text-sm font-medium mb-3">Rule Behavior</h4>
          <div className="space-y-2 text-sm text-muted-foreground">
            <p>
              This rule will match{' '}
              {rule.type === 'user' ? 'users' : rule.type === 'group' ? 'groups' : 'applications'}{' '}
              named "{rule.matches}".
            </p>
            <p>
              When matched,{' '}
              {(() => {
                const policyDesc = getPolicyDescription(rule.policy);
                return (
                  policyDesc?.behavior ||
                  `the application will be placed according to the ${getPolicyDisplay(rule.policy).toLowerCase()} policy`
                );
              })()}
              {rule.parentQueue && ` under the parent queue "${rule.parentQueue}"`}.
            </p>
            {rule.create && (
              <p className="text-amber-600 dark:text-amber-400">
                ⚠️ If the target queue doesn't exist, it will be created automatically.
              </p>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
