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
 * Dialog component for migrating legacy placement rules to JSON format
 */

import { useState } from 'react';
import { Button } from '~/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Alert, AlertDescription } from '~/components/ui/alert';
import { AlertCircle, CheckCircle2 } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { MigrationResult } from '~/types/features/placement-rules';

interface PlacementRulesMigrationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export const PlacementRulesMigrationDialog = ({
  open,
  onOpenChange,
}: PlacementRulesMigrationDialogProps) => {
  // State values (trigger re-renders only when these specific values change)
  const { legacyRules } = useSchedulerStore(
    useShallow((s) => ({
      legacyRules: s.legacyRules,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const storeMigrateLegacyRules = useSchedulerStore((s) => s.migrateLegacyRules);

  const [migrationResult, setMigrationResult] = useState<MigrationResult | null>(null);
  const [isMigrating, setIsMigrating] = useState(false);

  const handleMigrate = async () => {
    if (!legacyRules) return;

    setIsMigrating(true);
    try {
      // Use the store's migration function which handles both rules and format
      await storeMigrateLegacyRules();

      // Set success result for UI feedback
      setMigrationResult({
        success: true,
        rules: [],
        errors: [],
      });

      // Close dialog after successful migration
      setTimeout(() => {
        onOpenChange(false);
        setMigrationResult(null);
      }, 2000);
    } catch (error) {
      setMigrationResult({
        success: false,
        rules: [],
        errors: [error instanceof Error ? error.message : 'Migration failed'],
      });
    } finally {
      setIsMigrating(false);
    }
  };

  const handleCancel = () => {
    onOpenChange(false);
    setMigrationResult(null);
  };

  // Handle dialog state changes
  const handleDialogOpenChange = (newOpen: boolean) => {
    if (!newOpen) {
      setMigrationResult(null);
    }
    onOpenChange(newOpen);
  };

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent className="sm:max-w-[625px]">
        <DialogHeader>
          <DialogTitle>Migrate Legacy Placement Rules</DialogTitle>
          <DialogDescription>
            Legacy placement rules have been detected. We recommend migrating to the new JSON format
            for better flexibility and features.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              The migration will automatically convert your existing rules and stage the changes for
              review. You can review and modify the migrated rules before applying them.
            </AlertDescription>
          </Alert>

          {legacyRules && (
            <div className="space-y-2">
              <h4 className="text-sm font-medium">Current Legacy Rules:</h4>
              <pre className="rounded-md bg-muted p-3 text-xs overflow-x-auto max-h-40">
                {legacyRules}
              </pre>
            </div>
          )}

          {migrationResult && (
            <div className="space-y-2">
              {migrationResult.success ? (
                <Alert className="border-green-200 bg-green-50 dark:border-green-800 dark:bg-green-950">
                  <CheckCircle2 className="h-4 w-4 text-green-600" />
                  <AlertDescription className="text-green-800 dark:text-green-200">
                    Successfully converted {migrationResult.rules.length} rules. Changes have been
                    staged for review.
                  </AlertDescription>
                </Alert>
              ) : (
                <Alert variant="destructive">
                  <AlertDescription>
                    Migration failed:
                    <ul className="mt-2 list-disc list-inside">
                      {migrationResult.errors.map((error) => (
                        <li key={error}>{error}</li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              )}
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel} disabled={isMigrating}>
            Keep Legacy Rules
          </Button>
          <Button
            onClick={handleMigrate}
            disabled={isMigrating || !legacyRules || migrationResult?.success}
          >
            {isMigrating ? 'Migrating...' : 'Migrate to JSON'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
