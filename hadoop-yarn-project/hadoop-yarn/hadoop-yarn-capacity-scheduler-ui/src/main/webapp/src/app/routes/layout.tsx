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


import { Outlet, useLocation } from 'react-router';
import { useState, useEffect } from 'react';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { StagedChangesPanel } from '~/features/staged-changes/components/StagedChangesPanel';
import { AppSidebar } from '~/components/layouts/app-sidebar';
import { ModeToggle } from '~/components/elements/mode-toggle';
import { GlobalRefreshButton } from '~/components/elements/GlobalRefreshButton';
import { DiagnosticsDialog } from '~/components/elements/DiagnosticsDialog';
import { SidebarProvider, SidebarInset, SidebarTrigger } from '~/components/ui/sidebar';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { Lock, GitCompareArrows } from 'lucide-react';
import { SearchBar } from '~/components/search/SearchBar';
import { useKeyboardShortcuts } from '~/hooks/useKeyboardShortcuts';
import { toast } from 'sonner';
import { READ_ONLY_PROPERTY } from '~/config';

export default function Layout() {
  const [stagedChangesPanelOpen, setStagedChangesPanelOpen] = useState(false);
  const [isApplying, setIsApplying] = useState(false);
  const loadInitialData = useSchedulerStore((state) => state.loadInitialData);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const setSearchContext = useSchedulerStore((state) => state.setSearchContext);
  const isReadOnly = useSchedulerStore((state) => state.isReadOnly);
  const applyChanges = useSchedulerStore((state) => state.applyChanges);
  const clearAllChanges = useSchedulerStore((state) => state.clearAllChanges);
  const isPropertyPanelOpen = useSchedulerStore((state) => state.isPropertyPanelOpen);
  const isComparisonModeActive = useSchedulerStore((state) => state.isComparisonModeActive);
  const toggleComparisonMode = useSchedulerStore((state) => state.toggleComparisonMode);
  const location = useLocation();

  useEffect(() => {
    loadInitialData().catch((err) => {
      console.error('Failed to load initial data:', err);
    });
  }, [loadInitialData]);

  // Update search context based on current route
  useEffect(() => {
    if (location.pathname === '/') {
      setSearchContext('queues');
    } else if (location.pathname === '/node-labels') {
      setSearchContext('nodes');
    } else if (location.pathname === '/global-settings') {
      setSearchContext('settings');
    } else {
      setSearchContext(null);
    }
  }, [location.pathname, setSearchContext]);

  // Calculate if there are validation errors blocking apply
  const hasValidationErrors = stagedChanges.some((change) =>
    change.validationErrors?.some((error) => error.severity === 'error'),
  );

  // Global keyboard shortcuts for staged changes
  useKeyboardShortcuts([
    {
      key: 's',
      ctrl: true,
      meta: true,
      preventDefault: true,
      handler: async () => {
        // Don't trigger if property panel is open (let PropertyPanel handle it)
        if (isPropertyPanelOpen) {
          return;
        }

        // If no staged changes, inform user
        if (stagedChanges.length === 0) {
          return;
        }

        // If panel is closed, open it
        if (!stagedChangesPanelOpen) {
          setStagedChangesPanelOpen(true);
          toast.info('Staged changes panel opened. Press Cmd/Ctrl+S again to apply changes.');
          return;
        }

        // If panel is open and there are changes without validation errors, apply them
        if (!isApplying && !hasValidationErrors && !isReadOnly) {
          setIsApplying(true);
          try {
            await applyChanges();
            toast.success('All changes applied successfully');
            setStagedChangesPanelOpen(false);
          } catch (error) {
            toast.error('Failed to apply changes');
            console.error('Failed to apply changes:', error);
          } finally {
            setIsApplying(false);
          }
        } else if (hasValidationErrors) {
          toast.error('Cannot apply changes with validation errors');
        } else if (isReadOnly) {
          toast.error('Cannot apply changes in read-only mode');
        }
      },
    },
    {
      key: 'k',
      ctrl: true,
      meta: true,
      preventDefault: true,
      handler: () => {
        // Only trigger when staged changes panel is open and property panel is not
        if (stagedChangesPanelOpen && !isPropertyPanelOpen && stagedChanges.length > 0) {
          clearAllChanges();
          toast.info('All staged changes cleared');
        }
      },
    },
  ]);

  // Determine page title and description based on current route
  const getPageInfo = () => {
    if (location.pathname === '/') {
      return {
        title: 'Queue Hierarchy',
        description: 'Visualize and manage your YARN Capacity Scheduler queues',
      };
    } else if (location.pathname === '/global-settings') {
      return {
        title: 'Global Settings',
        description: 'Configure scheduler-wide settings and properties',
      };
    } else if (location.pathname === '/node-labels') {
      return {
        title: 'Node Labels',
        description: 'Manage node labels and node-to-label mappings',
      };
    } else if (location.pathname === '/placement-rules') {
      return {
        title: 'Placement Rules',
        description: 'Define rules for application placement in queues',
      };
    }
    return { title: '', description: '' };
  };

  const pageInfo = getPageInfo();

  return (
    <SidebarProvider
      style={
        {
          '--sidebar-width': '16rem',
        } as React.CSSProperties
      }
    >
      <AppSidebar />
      <SidebarInset>
        <div className="flex flex-1 flex-col">
          {/* Main content area */}
          <div className="flex-1 flex flex-col">
            {/* Header with page title */}
            <header className="flex h-20 items-center gap-4 border-b bg-gradient-to-r from-background to-muted/20 px-6">
              <SidebarTrigger />
              <div className="flex-1">
                <h1 className="text-2xl font-bold tracking-tight">{pageInfo.title}</h1>
                <p className="text-sm text-muted-foreground">{pageInfo.description}</p>
              </div>
              <SearchBar className="w-72" placeholder="Search" />
              {isReadOnly && (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge variant="destructive" className="gap-1.5">
                        <Lock className="h-3 w-3" />
                        Read-Only
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>Set {READ_ONLY_PROPERTY}=false in YARN to enable editing</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
              <GlobalRefreshButton />
              {location.pathname === '/' && (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Button
                        variant={isComparisonModeActive ? 'default' : 'ghost'}
                        size="icon"
                        onClick={toggleComparisonMode}
                        aria-label={
                          isComparisonModeActive ? 'Exit comparison mode' : 'Enter comparison mode'
                        }
                      >
                        <GitCompareArrows className="h-4 w-4" />
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>{isComparisonModeActive ? 'Exit comparison mode' : 'Compare queues'}</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
              <DiagnosticsDialog />
              <ModeToggle />
            </header>

            {/* Page content */}
            <div className="flex-1 overflow-hidden">
              <Outlet />
            </div>
          </div>
        </div>
      </SidebarInset>

      <StagedChangesPanel
        open={stagedChangesPanelOpen}
        onClose={() => setStagedChangesPanelOpen(false)}
        onOpen={() => setStagedChangesPanelOpen(true)}
      />
    </SidebarProvider>
  );
}
