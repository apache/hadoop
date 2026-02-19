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
 * Queue validation badges
 *
 * Displays validation error and warning badges with tooltips for queue cards.
 */

import React from 'react';
import { AlertCircle, AlertTriangle } from 'lucide-react';
import { Badge } from '~/components/ui/badge';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import type { ValidationIssue } from '~/types';
import { splitIssues } from '~/features/validation/service';

interface QueueValidationBadgesProps {
  validationErrors?: ValidationIssue[];
  isAffectedByErrors?: boolean;
  errorSource?: string;
}

export const QueueValidationBadges: React.FC<QueueValidationBadgesProps> = ({
                                                                              validationErrors,
                                                                              isAffectedByErrors,
                                                                              errorSource,
                                                                            }) => {
  if (!validationErrors && !isAffectedByErrors) {
    return null;
  }

  const { errors, warnings } = validationErrors
      ? splitIssues(validationErrors)
      : { errors: [], warnings: [] };

  return (
      <div className="flex items-center gap-1.5 ml-2">
        {/* Direct errors badge */}
        {errors.length > 0 && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <Badge variant="destructive" className="h-6 px-2" aria-label={`${errors.length} validation error${errors.length === 1 ? '' : 's'}`}>
                    <AlertCircle className="h-3 w-3 mr-1" />
                    {errors.length}
                  </Badge>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="font-semibold mb-1">Validation Errors</p>
                  <ul className="text-sm space-y-1">
                    {errors.map((error) => (
                        <li key={`${error.field}-${error.message}`}>• {error.message}</li>
                    ))}
                  </ul>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
        )}

        {/* Direct warnings badge */}
        {warnings.length > 0 && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <Badge
                      variant="outline"
                      className="h-6 px-2 border-amber-500 text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/30"
                      aria-label={`${warnings.length} validation warning${warnings.length === 1 ? '' : 's'}`}
                  >
                    <AlertTriangle className="h-3 w-3 mr-1" />
                    {warnings.length}
                  </Badge>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="font-semibold mb-1">Validation Warnings</p>
                  <ul className="text-sm space-y-1">
                    {warnings.map((warning) => (
                        <li key={`${warning.field}-${warning.message}`}>• {warning.message}</li>
                    ))}
                  </ul>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
        )}

        {/* Affected by child issues badge */}
        {isAffectedByErrors && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <Badge
                      variant="outline"
                      className="h-6 px-2 border-orange-500 text-orange-600 dark:text-orange-400 bg-orange-50 dark:bg-orange-950/30"
                      aria-label={`Affected by validation issues from ${errorSource ? `queue "${errorSource}"` : 'child queues'}`}
                  >
                    <AlertTriangle className="h-3 w-3 mr-1" />
                    Child
                  </Badge>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="font-semibold mb-1">Affected by Child Queue Changes</p>
                  <p className="text-sm">
                    This queue is affected by validation issues from{' '}
                    {errorSource ? `queue "${errorSource}"` : 'child queues'}.
                  </p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
        )}
      </div>
  );
};
