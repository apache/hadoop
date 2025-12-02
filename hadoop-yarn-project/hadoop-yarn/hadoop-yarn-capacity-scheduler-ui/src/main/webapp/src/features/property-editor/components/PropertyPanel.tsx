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


import React, { useState, useEffect, useRef } from 'react';
import { Save, RotateCcw, GitBranch, Info, Settings, Edit, AlertTriangle } from 'lucide-react';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '~/components/ui/sheet';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '~/components/ui/tabs';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import { QueueOverview } from './QueueOverview';
import { QueueInfoTab } from './QueueInfoTab';
import { PropertyEditorTab } from './PropertyEditorTab';
import { UnsavedChangesDialog } from './dialogs/UnsavedChangesDialog';
import { ValidationIssuesPopover } from './ValidationIssuesPopover';
import type { PropertyEditorTabHandle } from './PropertyEditorTab';
import { toast } from 'sonner';
import { useValidation } from '~/contexts/ValidationContext';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { cn } from '~/utils/cn';
import { TemplateConfigDialog } from '~/features/template-config/components/TemplateConfigDialog';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import { useKeyboardShortcuts, getModifierKey } from '~/hooks/useKeyboardShortcuts';
import { Kbd } from '~/components/ui/kbd';

export const PropertyPanel: React.FC = () => {
  const {
    selectedQueuePath,
    isPropertyPanelOpen,
    setPropertyPanelOpen,
    getQueueByPath,
    selectQueue,
    propertyPanelInitialTab,
    shouldOpenTemplateConfig,
    clearTemplateConfigRequest,
  } = useSchedulerStore();

  const getQueuePropertyValue = useSchedulerStore((state) => state.getQueuePropertyValue);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);

  const [tabValue, setTabValue] = useState('overview');
  const [hasChanges, setHasChanges] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isFormDirty, setIsFormDirty] = useState(false);
  const [showUnsavedDialog, setShowUnsavedDialog] = useState(false);
  const [pendingClose, setPendingClose] = useState(false);
  const [isSummaryOpen, setIsSummaryOpen] = useState(false);
  const [isTemplateDialogOpen, setIsTemplateDialogOpen] = useState(false);

  const propertyEditorRef = useRef<PropertyEditorTabHandle>(null);
  const { errors: validationState } = useValidation();

  const selectedQueue = selectedQueuePath ? getQueueByPath(selectedQueuePath) : null;
  const isPanelVisible = Boolean(selectedQueue && isPropertyPanelOpen);
  const isAutoCreatedQueue =
    selectedQueue?.creationMethod === 'dynamicLegacy' ||
    selectedQueue?.creationMethod === 'dynamicFlexible';
  const isSettingsDisabled = Boolean(isAutoCreatedQueue);

  const hasLegacyStagedEnable = Boolean(
    selectedQueuePath &&
      stagedChanges?.some(
        (change) =>
          change.queuePath === selectedQueuePath &&
          change.property === AUTO_CREATION_PROPS.LEGACY_ENABLED &&
          change.newValue === 'true',
      ),
  );

  const hasFlexibleStagedEnable = Boolean(
    selectedQueuePath &&
      stagedChanges?.some(
        (change) =>
          change.queuePath === selectedQueuePath &&
          change.property === AUTO_CREATION_PROPS.FLEXIBLE_ENABLED &&
          change.newValue === 'true',
      ),
  );

  const legacyTemplateAvailable = Boolean(
    selectedQueuePath &&
      (getQueuePropertyValue(selectedQueuePath, AUTO_CREATION_PROPS.LEGACY_ENABLED).value ===
        'true' ||
        hasLegacyStagedEnable),
  );

  const flexibleTemplateAvailable = Boolean(
    selectedQueuePath &&
      (getQueuePropertyValue(selectedQueuePath, AUTO_CREATION_PROPS.FLEXIBLE_ENABLED).value ===
        'true' ||
        hasFlexibleStagedEnable),
  );

  const showTemplateButton = Boolean(
    selectedQueuePath && (legacyTemplateAvailable || flexibleTemplateAvailable),
  );

  useEffect(() => {
    if (isPropertyPanelOpen) {
      const initialTab =
        isSettingsDisabled && propertyPanelInitialTab === 'settings'
          ? 'overview'
          : propertyPanelInitialTab;
      setTabValue(initialTab);
    }
  }, [isPropertyPanelOpen, propertyPanelInitialTab, isSettingsDisabled]);

  useEffect(() => {
    if (isSettingsDisabled && tabValue === 'settings') {
      setTabValue('overview');
    }
  }, [isSettingsDisabled, tabValue]);

  useEffect(() => {
    if (!isPropertyPanelOpen) {
      setIsTemplateDialogOpen(false);
    }
  }, [isPropertyPanelOpen]);

  useEffect(() => {
    if (!shouldOpenTemplateConfig) {
      return;
    }
    if (showTemplateButton) {
      setIsTemplateDialogOpen(true);
    }
    clearTemplateConfigRequest();
  }, [shouldOpenTemplateConfig, showTemplateButton, clearTemplateConfigRequest]);

  const handleClose = (force = false) => {
    if (!force && isFormDirty && tabValue === 'settings') {
      setShowUnsavedDialog(true);
      setPendingClose(true);
    } else {
      setPropertyPanelOpen(false);
      selectQueue(null); // Deselect queue when panel closes
      setPendingClose(false);
    }
  };

  const handleSubmit = async () => {
    if (propertyEditorRef.current) {
      // Check if form is valid before submitting
      if (!propertyEditorRef.current.isValid()) {
        toast.error('Please fix validation errors before staging changes');
        setIsSummaryOpen(true);
        return;
      }

      if (isFormDirty) {
        await propertyEditorRef.current.submit();
        // The submit function will show its own toast
      } else if (hasChanges) {
        // If there are already staged changes but no new changes, show info
        toast.info('Changes are already staged. Use the bottom drawer to apply all changes.');
      }
    }
  };

  const handleReset = () => {
    if (propertyEditorRef.current) {
      propertyEditorRef.current.reset();
    }
    setIsSummaryOpen(false);
  };

  const handleHasChangesChange = (newHasChanges: boolean) => {
    setHasChanges(newHasChanges);
  };

  const handleFormDirtyChange = (newIsFormDirty: boolean) => {
    setIsFormDirty(newIsFormDirty);
  };

  const handleIsSubmittingChange = (newIsSubmitting: boolean) => {
    setIsSubmitting(newIsSubmitting);
  };

  const handleSaveAndClose = async () => {
    // Check if form is valid
    if (propertyEditorRef.current && !propertyEditorRef.current.isValid()) {
      toast.error('Please fix validation errors before saving');
      setIsSummaryOpen(true);
      return; // Don't close the dialog or panel
    }

    await handleSubmit();
    if (pendingClose) {
      handleClose(true);
    }
    setShowUnsavedDialog(false);
  };

  const handleDiscardAndClose = () => {
    handleReset();
    handleClose(true);
    setShowUnsavedDialog(false);
  };

  // Reset hasChanges and form dirty state when panel opens/closes or queue changes
  useEffect(() => {
    if (!isPropertyPanelOpen || !selectedQueuePath) {
      setHasChanges(false);
      setIsFormDirty(false);
      setIsSummaryOpen(false);
    }
  }, [isPropertyPanelOpen, selectedQueuePath]);

  useEffect(() => {
    if (!isPropertyPanelOpen) {
      setIsSummaryOpen(false);
    }
  }, [isPropertyPanelOpen]);

  const queuePath = selectedQueue?.queuePath;

  const queueIssues = !queuePath ? {} : (validationState[queuePath] ?? {});

  const issueList = !queuePath
    ? []
    : Object.entries(queueIssues).flatMap(([field, issues]) =>
        issues.map((issue, index) => ({
          ...issue,
          field,
          key: `${field}-${issue.rule}-${index}`,
        })),
      );

  const handleIssueSelect = (field: string) => {
    const selector = `[data-field-id="${field.replace(/"/g, '\\"')}"]`;
    const element = document.querySelector(selector) as HTMLElement | null;
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'center' });
      element.focus?.();
    }
    setIsSummaryOpen(false);
  };

  // Keyboard shortcuts - only active when panel is open and on settings tab
  useKeyboardShortcuts(
    isPanelVisible && tabValue === 'settings'
      ? [
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: () => {
              if (!isSubmitting && (hasChanges || isFormDirty)) {
                handleSubmit();
              }
            },
          },
          {
            key: 'k',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: () => {
              if (!isSubmitting && (hasChanges || isFormDirty)) {
                handleReset();
              }
            },
          },
        ]
      : [],
  );

  if (!isPanelVisible || !selectedQueue) {
    return null;
  }

  return (
    <>
      <Sheet open={isPropertyPanelOpen} onOpenChange={handleClose}>
        <SheetContent
          side="right"
          className="sm:max-w-[420px]"
          onOpenAutoFocus={(e) => e.preventDefault()}
        >
          <div className="flex flex-col h-full relative overflow-hidden">
            <SheetHeader>
              <div className="space-y-1">
                <SheetTitle className="text-base font-semibold">
                  Queue: {selectedQueue.queueName}
                </SheetTitle>
              </div>
              {/* Status bar */}
              <div className="mt-2 border-b">
                <div className="flex items-center gap-2 pb-2">
                  <span className="text-xs text-muted-foreground">{selectedQueue.queuePath}</span>
                  <div className="flex-1" />
                  <ValidationIssuesPopover
                    isOpen={isSummaryOpen}
                    onOpenChange={setIsSummaryOpen}
                    issues={issueList}
                    onIssueSelect={handleIssueSelect}
                  />
                  {isFormDirty && (
                    <Badge variant="outline" className="text-xs">
                      <Edit className="h-3 w-3 mr-1" />
                      Unsaved
                    </Badge>
                  )}
                  {!isFormDirty && hasChanges && (
                    <Badge variant="default" className="text-xs">
                      <Edit className="h-3 w-3 mr-1" />
                      Staged
                    </Badge>
                  )}
                </div>
                {/* Validation summary handled via popover */}
              </div>
            </SheetHeader>

            {isAutoCreatedQueue && (
              <Alert className="mt-3 border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900/60 dark:bg-amber-950 dark:text-amber-100">
                <AlertTriangle className="h-4 w-4" />
                <AlertTitle>Auto-created queue</AlertTitle>
                <AlertDescription>
                  This queue was created automatically by the scheduler and its settings are managed
                  externally. The Settings tab is disabled while it remains auto-managed.
                </AlertDescription>
              </Alert>
            )}

            <Tabs value={tabValue} onValueChange={setTabValue} className="flex-1 overflow-hidden">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="overview">
                  <GitBranch className="h-4 w-4 mr-2" />
                  Overview
                </TabsTrigger>
                <TabsTrigger value="info">
                  <Info className="h-4 w-4 mr-2" />
                  Info
                </TabsTrigger>
                <TabsTrigger
                  value="settings"
                  disabled={isSettingsDisabled}
                  className={cn(
                    isSettingsDisabled && 'cursor-not-allowed opacity-60 hover:opacity-60',
                  )}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  Settings
                </TabsTrigger>
              </TabsList>

              <TabsContent value="overview" className="h-full overflow-auto">
                <QueueOverview queue={selectedQueue} />
              </TabsContent>
              <TabsContent value="info" className="h-full overflow-auto">
                <QueueInfoTab queue={selectedQueue} />
              </TabsContent>
              <TabsContent value="settings" className="h-full overflow-auto pb-20">
                <PropertyEditorTab
                  ref={propertyEditorRef}
                  queue={selectedQueue}
                  onHasChangesChange={handleHasChangesChange}
                  onIsSubmittingChange={handleIsSubmittingChange}
                  onFormDirtyChange={handleFormDirtyChange}
                  templateConfigControls={
                    showTemplateButton
                      ? {
                          canManageTemplates: showTemplateButton,
                          legacyAvailable: legacyTemplateAvailable,
                          flexibleAvailable: flexibleTemplateAvailable,
                          onOpenTemplateConfig: () => setIsTemplateDialogOpen(true),
                        }
                      : undefined
                  }
                />
              </TabsContent>
            </Tabs>

            {/* Fixed Apply/Reset buttons - show on Settings tab */}
            {tabValue === 'settings' && (
              <div className="sticky bottom-0 left-0 right-0 mt-auto p-4 bg-background border-t flex gap-2 justify-end">
                <Button
                  variant="outline"
                  onClick={handleReset}
                  disabled={isSubmitting || (!hasChanges && !isFormDirty)}
                  className="gap-2"
                >
                  <RotateCcw className="h-4 w-4" />
                  Reset
                  <Kbd className="ml-auto">{getModifierKey()}+K</Kbd>
                </Button>
                <Button
                  variant="default"
                  onClick={handleSubmit}
                  disabled={isSubmitting || (!hasChanges && !isFormDirty)}
                  className="gap-2"
                >
                  {isSubmitting ? (
                    <>
                      <div className="h-4 w-4 animate-spin rounded-full border-2 border-background border-t-transparent" />
                      Staging...
                    </>
                  ) : (
                    <>
                      <Save className="h-4 w-4" />
                      {isFormDirty ? 'Stage Changes' : 'No Changes'}
                      <Kbd className="ml-auto">{getModifierKey()}+S</Kbd>
                    </>
                  )}
                </Button>
              </div>
            )}
          </div>
        </SheetContent>
      </Sheet>

      <UnsavedChangesDialog
        open={showUnsavedDialog}
        onOpenChange={setShowUnsavedDialog}
        onSave={handleSaveAndClose}
        onDiscard={handleDiscardAndClose}
        isSaving={isSubmitting}
      />

      {selectedQueuePath && (
        <TemplateConfigDialog
          open={isTemplateDialogOpen}
          queuePath={selectedQueuePath}
          onClose={() => setIsTemplateDialogOpen(false)}
        />
      )}
    </>
  );
};
