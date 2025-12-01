/**
 * Queue card context menu
 *
 * Context menu for queue card actions like edit, add, delete, and state toggle.
 */

import React from 'react';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from '~/components/ui/context-menu';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { Plus, Trash2, Edit, Play, Pause, SlidersHorizontal, FileCog } from 'lucide-react';
import { QUEUE_STATES, SPECIAL_VALUES } from '~/types';

type StagedStatus = 'new' | 'modified' | 'deleted';

interface QueueCardContextMenuProps {
  children: React.ReactNode;
  queuePath: string;
  state?: string;
  stagedStatus?: StagedStatus;
  isAutoCreatedQueue?: boolean;
  isTemplateManageable?: boolean;
  canAdd: boolean;
  canDelete: boolean;
  hasPendingDeletion: boolean;
  isSelectedQueue: boolean;
  isPropertyPanelOpen: boolean;
  onEditProperties: (event: React.MouseEvent) => void;
  onManageTemplate: (event: React.MouseEvent) => void;
  onEditCapacity: (event: React.MouseEvent) => void;
  onToggleState: () => void;
  onAddChild: (event: React.MouseEvent) => void;
  onDelete: (event: React.MouseEvent) => void;
  onRemoveStaged: (event: React.MouseEvent) => void;
  onOpenChange: (open: boolean) => void;
}

export const QueueCardContextMenu: React.FC<QueueCardContextMenuProps> = ({
  children,
  queuePath,
  state,
  stagedStatus,
  isAutoCreatedQueue,
  isTemplateManageable,
  canAdd,
  canDelete,
  hasPendingDeletion,
  isSelectedQueue,
  isPropertyPanelOpen,
  onEditProperties,
  onManageTemplate,
  onEditCapacity,
  onToggleState,
  onAddChild,
  onDelete,
  onRemoveStaged,
  onOpenChange,
}) => {
  const isRoot = queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;
  const canToggleState = !isRoot;
  const isRunning = state === QUEUE_STATES.RUNNING;
  const isTemplateActionDisabled = stagedStatus === 'new' || isAutoCreatedQueue;

  const handleOpenChange = (open: boolean) => {
    // Deselect queue when context menu closes
    if (!open && isSelectedQueue && !isPropertyPanelOpen) {
      onOpenChange(open);
    }
  };

  const contextMenuContent = (
    <ContextMenuContent className="w-48">
      <ContextMenuItem
        onClick={(e) => {
          e.stopPropagation();
          onEditProperties(e);
        }}
        disabled={stagedStatus === 'new' || isAutoCreatedQueue}
      >
        <Edit className="mr-2 h-4 w-4" />
        Edit Properties
      </ContextMenuItem>

      {isTemplateManageable && (
        <ContextMenuItem
          onClick={(e) => {
            e.stopPropagation();
            onManageTemplate(e);
          }}
          disabled={isTemplateActionDisabled}
        >
          <FileCog className="mr-2 h-4 w-4" />
          Manage Template Properties
        </ContextMenuItem>
      )}

      {!isRoot && (
        <ContextMenuItem onClick={(e) => onEditCapacity(e)}>
          <SlidersHorizontal className="mr-2 h-4 w-4" />
          Edit Capacity
        </ContextMenuItem>
      )}

      {stagedStatus === 'new' && !isRoot && (
        <ContextMenuItem onClick={onRemoveStaged} className="text-red-600 focus:text-red-600">
          <Trash2 className="mr-2 h-4 w-4" />
          Remove Staged Queue
        </ContextMenuItem>
      )}

      <ContextMenuItem
        onClick={(e) => {
          e.stopPropagation();
          onToggleState();
        }}
        disabled={!canToggleState}
      >
        {isRunning ? (
          <>
            <Pause className="mr-2 h-4 w-4" />
            Stop Queue
          </>
        ) : (
          <>
            <Play className="mr-2 h-4 w-4" />
            Start Queue
          </>
        )}
      </ContextMenuItem>

      {canAdd && stagedStatus !== 'new' && (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <ContextMenuItem
                onClick={(e) => {
                  e.stopPropagation();
                  onAddChild(e);
                }}
                disabled={hasPendingDeletion}
              >
                <Plus className="mr-2 h-4 w-4" />
                Add Child Queue
              </ContextMenuItem>
            </TooltipTrigger>
            {hasPendingDeletion && (
              <TooltipContent>
                <p>Cannot add children to queue pending deletion</p>
              </TooltipContent>
            )}
          </Tooltip>
        </TooltipProvider>
      )}

      {canDelete && stagedStatus !== 'new' && (
        <>
          <ContextMenuSeparator />
          <ContextMenuItem
            onClick={(e) => {
              e.stopPropagation();
              onDelete(e);
            }}
            className="text-red-600 focus:text-red-600"
          >
            <Trash2 className="mr-2 h-4 w-4" />
            Delete Queue
          </ContextMenuItem>
        </>
      )}
    </ContextMenuContent>
  );

  return (
    <ContextMenu onOpenChange={handleOpenChange}>
      {stagedStatus === 'new' ? (
        <TooltipProvider>
          <Tooltip>
            <ContextMenuTrigger asChild>
              <TooltipTrigger asChild>{children}</TooltipTrigger>
            </ContextMenuTrigger>
            <TooltipContent>
              <p>This queue must be applied before it can be edited</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      ) : (
        <ContextMenuTrigger asChild>{children}</ContextMenuTrigger>
      )}

      {contextMenuContent}
    </ContextMenu>
  );
};
