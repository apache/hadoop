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


import { useState, useEffect } from 'react';
import { Button } from '~/components/ui/button';
import { Card, CardContent } from '~/components/ui/card';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { Plus, InfoIcon, AlertCircle } from 'lucide-react';
import { PlacementRuleForm } from './PlacementRuleForm';
import { PlacementRulesTable } from './PlacementRulesTable';
import { PolicyReferenceDialog } from './PolicyReferenceDialog';
import { PlacementRulesMigrationDialog } from './MigrationDialog';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { PlacementRule } from '~/types/features/placement-rules';

export function PlacementRulesList() {
  // State values (trigger re-renders only when these specific values change)
  const {
    rules,
    selectedRuleIndex,
    isLegacyMode,
    legacyRules,
    configData,
    stagedChanges,
    formatWarning,
    applyError,
  } = useSchedulerStore(
    useShallow((s) => ({
      rules: s.rules,
      selectedRuleIndex: s.selectedRuleIndex,
      isLegacyMode: s.isLegacyMode,
      legacyRules: s.legacyRules,
      configData: s.configData,
      stagedChanges: s.stagedChanges,
      formatWarning: s.formatWarning,
      applyError: s.applyError,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const addRule = useSchedulerStore((s) => s.addRule);
  const deleteRule = useSchedulerStore((s) => s.deleteRule);
  const reorderRules = useSchedulerStore((s) => s.reorderRules);
  const selectRule = useSchedulerStore((s) => s.selectRule);
  const loadPlacementRules = useSchedulerStore((s) => s.loadPlacementRules);

  const [showAddForm, setShowAddForm] = useState(false);
  const [showMigrationDialog, setShowMigrationDialog] = useState(false);

  // Load placement rules when component mounts and config data is available
  // Also re-run when staged changes update to properly detect migration status
  useEffect(() => {
    // Only load placement rules if we have config data
    if (configData && configData.size > 0) {
      loadPlacementRules();
    }
  }, [configData, stagedChanges, loadPlacementRules]);

  // Reset dialog when transitioning out of legacy mode
  useEffect(() => {
    if (!isLegacyMode) {
      setShowMigrationDialog(false);
    }
  }, [isLegacyMode]);

  const handleAdd = (data: PlacementRule) => {
    addRule(data);
    setShowAddForm(false);
  };

  if (showAddForm && !isLegacyMode) {
    return (
      <div className="space-y-4">
        {applyError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Failed to Apply Changes</AlertTitle>
            <AlertDescription>{applyError}</AlertDescription>
          </Alert>
        )}
        <PlacementRuleForm onSubmit={handleAdd} onCancel={() => setShowAddForm(false)} />
      </div>
    );
  }

  // Show legacy mode UI
  if (isLegacyMode) {
    return (
      <div className="space-y-4">
        {applyError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Failed to Apply Changes</AlertTitle>
            <AlertDescription>{applyError}</AlertDescription>
          </Alert>
        )}
        <Card className="border-warning">
          <CardContent className="pt-6">
            <div className="flex flex-col items-center space-y-4">
              <div className="text-center">
                <h3 className="text-lg font-semibold mb-2">Legacy Placement Rules Detected</h3>
                <p className="text-muted-foreground mb-4">
                  This scheduler is using legacy placement rules format. The visual editor is not
                  available for legacy rules.
                </p>
                {legacyRules && (
                  <div className="mb-4">
                    <p className="text-sm font-medium mb-2">Current legacy rules:</p>
                    <pre className="rounded-md bg-muted p-3 text-xs overflow-x-auto max-w-2xl mx-auto">
                      {legacyRules}
                    </pre>
                  </div>
                )}
                <p className="text-sm text-muted-foreground mb-4">
                  To use the visual placement rules editor, please migrate to the JSON format.
                </p>
              </div>
              <Button onClick={() => setShowMigrationDialog(true)}>
                <InfoIcon className="h-4 w-4 mr-2" />
                Migrate to JSON Format
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Migration dialog for legacy rules */}
        <PlacementRulesMigrationDialog
          open={showMigrationDialog}
          onOpenChange={setShowMigrationDialog}
        />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {applyError && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Failed to Apply Changes</AlertTitle>
          <AlertDescription>{applyError}</AlertDescription>
        </Alert>
      )}
      {!isLegacyMode && formatWarning && (
        <Alert className="border-warning/70">
          <AlertCircle className="h-4 w-4 text-warning" />
          <AlertDescription className="text-warning">{formatWarning}</AlertDescription>
        </Alert>
      )}

      <div className="space-y-4">
        <Alert>
          <InfoIcon className="h-4 w-4" />
          <AlertDescription>
            Placement rules determine how the queue path is constructed for matching applications.
            Rules are evaluated from top to bottom, and the first matching rule determines the queue
            assignment. Drag rules to reorder them.
          </AlertDescription>
        </Alert>

        <div className="flex justify-end gap-2">
          <PolicyReferenceDialog />
          <Button onClick={() => setShowAddForm(true)}>
            <Plus className="h-4 w-4 mr-2" />
            Add Rule
          </Button>
        </div>
      </div>

      {rules.length === 0 ? (
        <Card className="border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <div className="text-center">
              <h3 className="text-lg font-medium mb-2">No placement rules configured</h3>
              <p className="text-muted-foreground mb-4">
                Applications will use the default queue assignment behavior
              </p>
              <Button onClick={() => setShowAddForm(true)}>
                <Plus className="h-4 w-4 mr-2" />
                Add First Rule
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <PlacementRulesTable
          rules={rules}
          selectedRuleIndex={selectedRuleIndex}
          onDelete={deleteRule}
          onSelect={selectRule}
          onReorder={reorderRules}
        />
      )}
    </div>
  );
}
