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


import { HelpCircle } from 'lucide-react';
import { Button } from '~/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '~/components/ui/dialog';
import { Badge } from '~/components/ui/badge';
import { Alert, AlertDescription } from '~/components/ui/alert';
import { CUSTOM_VARIABLES } from '~/features/placement-rules/constants/policy-descriptions';

interface CustomPlacementHelpDialogProps {
  triggerText?: string;
  showIcon?: boolean;
}

export function CustomPlacementHelpDialog({
  triggerText = 'Help',
  showIcon = true,
}: CustomPlacementHelpDialogProps) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="ghost" size="sm" className="gap-1.5">
          {showIcon && <HelpCircle className="h-3.5 w-3.5" />}
          {triggerText}
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:!max-w-4xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Custom Placement Variables</DialogTitle>
          <DialogDescription>
            These variables can be used if custom mapping policy is selected. The engine does only
            minimal verification when it comes to replacing them - therefore it is your
            responsibility to provide the correct string.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6">
          <Alert>
            <AlertDescription>
              The string must be resolved to a valid queue path in order to have a proper placement.
              You can combine variables with static text to create complex queue hierarchies.
            </AlertDescription>
          </Alert>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold">Available Variables</h3>
            <div className="space-y-3">
              {CUSTOM_VARIABLES.map((variable) => (
                <div key={variable.variable} className="rounded-lg border p-3 space-y-2">
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary" className="font-mono">
                      {variable.variable}
                    </Badge>
                    <span className="text-sm font-medium">{variable.meaning}</span>
                  </div>
                  <p className="text-sm text-muted-foreground">Example: {variable.example}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold">Usage Examples</h3>
            <div className="space-y-3">
              <div className="rounded-lg bg-muted p-3 space-y-1">
                <code className="text-sm font-mono">%specified</code>
                <p className="text-sm text-muted-foreground">
                  Use the queue specified during submission (equivalent to "specified" policy)
                </p>
              </div>

              <div className="rounded-lg bg-muted p-3 space-y-1">
                <code className="text-sm font-mono">%specified.%user.largejobs</code>
                <p className="text-sm text-muted-foreground">
                  Creates a hierarchy like: root.users.mrjobs.alice.largejobs
                </p>
              </div>

              <div className="rounded-lg bg-muted p-3 space-y-1">
                <code className="text-sm font-mono">root.%primary_group.%application</code>
                <p className="text-sm text-muted-foreground">
                  Groups applications by primary group and app name
                </p>
              </div>

              <div className="rounded-lg bg-muted p-3 space-y-1">
                <code className="text-sm font-mono">%default.%user</code>
                <p className="text-sm text-muted-foreground">
                  Places user queues under the current default queue
                </p>
              </div>
            </div>
          </div>

          <Alert variant="destructive">
            <AlertDescription>
              <strong>Important:</strong> Many policies can be achieved with custom placement. For
              example, instead of using the "specified" policy, you can use custom with
              <code className="mx-1">%specified</code>. However, custom placement gives you greater
              control by allowing you to append or prepend strings to variables.
            </AlertDescription>
          </Alert>
        </div>
      </DialogContent>
    </Dialog>
  );
}
