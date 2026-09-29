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


import React from 'react';
import { Link } from 'react-router';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '~/components/ui/dialog';
import { Badge } from '~/components/ui/badge';
import { Info, CheckCircle, XCircle, ArrowRight } from 'lucide-react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '~/components/ui/tabs';
import { Alert, AlertDescription } from '~/components/ui/alert';

interface LegacyModeDocumentationProps {
  children: React.ReactNode;
  legacyModeEnabled: boolean;
}

export const LegacyModeDocumentation: React.FC<LegacyModeDocumentationProps> = ({
  children,
  legacyModeEnabled,
}) => {
  return (
    <Dialog>
      <DialogTrigger asChild>{children}</DialogTrigger>
      <DialogContent className="sm:max-w-2xl lg:max-w-4xl max-h-[95vh]">
        <DialogHeader>
          <DialogTitle>YARN Capacity Scheduler Queue Modes</DialogTitle>
          <DialogDescription>
            Understanding Legacy Mode vs Flexible Mode in capacity configuration
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="overview" className="mt-4">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="legacy">Legacy Mode</TabsTrigger>
            <TabsTrigger value="flexible">Flexible Mode</TabsTrigger>
            <TabsTrigger value="migration">Migration</TabsTrigger>
          </TabsList>

          <div className="h-[600px] mt-4 overflow-y-auto">
            <TabsContent value="overview" className="space-y-4 pr-4">
              <h3 className="text-lg font-semibold">Queue Mode Overview</h3>
              <p className="text-sm text-muted-foreground">
                The YARN Capacity Scheduler supports two queue configuration modes that determine
                how capacity values are validated and enforced.
              </p>

              <Alert
                className={
                  legacyModeEnabled
                    ? 'border-warning bg-warning/10'
                    : 'border-green-600 bg-green-600/10'
                }
              >
                <CheckCircle
                  className={`h-4 w-4 ${legacyModeEnabled ? 'text-warning' : 'text-green-600'}`}
                />
                <AlertDescription>
                  <p className="font-semibold mb-1">
                    Currently Active: {legacyModeEnabled ? 'Legacy Mode' : 'Flexible Mode'}
                  </p>
                  <p className="text-sm">
                    {legacyModeEnabled
                      ? 'Strict capacity rules are enforced. All sibling queues must use the same capacity type and child capacities must sum to 100%.'
                      : 'Flexible capacity configuration is allowed. Queues can use different capacity types and child capacities do not need to sum to 100%.'}
                  </p>
                </AlertDescription>
              </Alert>

              <div className="space-y-3">
                <div className="border rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-2">
                    <Badge variant="warning">Legacy Mode</Badge>
                    <span className="text-sm text-muted-foreground">(Default)</span>
                  </div>
                  <p className="text-sm">
                    Enforces strict capacity rules for backward compatibility. All sibling queues
                    must use the same capacity type and child capacities must sum to exactly 100%.
                  </p>
                </div>

                <div className="border rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-2">
                    <Badge variant="success">Flexible Mode</Badge>
                    <span className="text-sm text-muted-foreground">
                      (Recommended for new clusters)
                    </span>
                  </div>
                  <p className="text-sm">
                    Allows mixed capacity types and flexible capacity sums. Provides more options
                    for resource allocation strategies.
                  </p>
                </div>
              </div>

              <Alert>
                <Info className="h-4 w-4" />
                <AlertDescription>
                  The mode is controlled by the{' '}
                  <code className="text-xs">yarn.scheduler.capacity.legacy-queue-mode.enabled</code>{' '}
                  property.{' '}
                  <Link to="/global-settings" className="underline hover:no-underline">
                    Configure in Global Settings →
                  </Link>
                </AlertDescription>
              </Alert>
            </TabsContent>

            <TabsContent value="legacy" className="space-y-4 pr-4">
              <h3 className="text-lg font-semibold">Legacy Mode Details</h3>
              <p className="text-sm text-muted-foreground">
                Legacy mode works like the previous YARN versions and enforces traditional capacity
                scheduling rules.
              </p>

              <div className="space-y-3">
                <h4 className="font-medium">Validation Rules</h4>

                <div className="space-y-2">
                  <div className="flex items-start gap-2">
                    <XCircle className="h-4 w-4 text-destructive mt-0.5 flex-shrink-0" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">Capacity Type Consistency</p>
                      <p className="text-xs text-muted-foreground">
                        All sibling queues must use the same capacity type (percentage, weight, or
                        absolute). You cannot mix different types at the same level.
                      </p>
                    </div>
                  </div>

                  <div className="flex items-start gap-2">
                    <XCircle className="h-4 w-4 text-destructive mt-0.5 flex-shrink-0" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">Child Capacity Sum</p>
                      <p className="text-xs text-muted-foreground">
                        When using percentage-based capacity, all child queues must sum to exactly
                        100%. No over or under allocation is allowed.
                      </p>
                    </div>
                  </div>
                </div>

                <h4 className="font-medium mt-4">Example Configuration</h4>
                <pre className="bg-muted p-3 rounded-md text-xs overflow-x-auto">
                  {`root
├── prod (60%)
│   ├── critical (70%)  # 70% of prod = 42% of root
│   └── regular (30%)   # 30% of prod = 18% of root
└── dev (40%)
    ├── team-a (50%)    # 50% of dev = 20% of root
    └── team-b (50%)    # 50% of dev = 20% of root`}
                </pre>

                <Alert className="mt-4">
                  <AlertDescription className="text-xs">
                    In legacy mode, you must ensure all percentages at each level sum to 100%. In
                    this example: prod (60%) + dev (40%) = 100%, critical (70%) + regular (30%) =
                    100%, etc.
                  </AlertDescription>
                </Alert>
              </div>
            </TabsContent>

            <TabsContent value="flexible" className="space-y-4 pr-4">
              <h3 className="text-lg font-semibold">Flexible Mode Details</h3>
              <p className="text-sm text-muted-foreground">
                Flexible mode provides more options for capacity configuration, allowing for dynamic
                and elastic resource allocation.
              </p>

              <div className="space-y-3">
                <h4 className="font-medium">Key Features</h4>

                <div className="space-y-2">
                  <div className="flex items-start gap-2">
                    <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400 mt-0.5 flex-shrink-0" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">Mixed Capacity Types</p>
                      <p className="text-xs text-muted-foreground">
                        Sibling queues can use different capacity types. Mix percentages, weights,
                        and absolute values as needed.
                      </p>
                    </div>
                  </div>

                  <div className="flex items-start gap-2">
                    <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400 mt-0.5 flex-shrink-0" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">Flexible Capacity Sum</p>
                      <p className="text-xs text-muted-foreground">
                        Child capacities don't need to sum to 100%. Over-allocation allows elastic
                        sharing, under-allocation reserves capacity.
                      </p>
                    </div>
                  </div>

                  <div className="flex items-start gap-2">
                    <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400 mt-0.5 flex-shrink-0" />
                    <div className="flex-1">
                      <p className="text-sm font-medium">Dynamic Resource Allocation</p>
                      <p className="text-xs text-muted-foreground">
                        Unused resources can be automatically redistributed based on demand when
                        using weight-based allocation.
                      </p>
                    </div>
                  </div>
                </div>

                <h4 className="font-medium mt-4">Example Configuration</h4>
                <pre className="bg-muted p-3 rounded-md text-xs overflow-x-auto">
                  {`root
├── prod (10w)          # Weight-based
│   ├── critical (80%)  # Percentage of prod
│   └── regular (40%)   # Can exceed 100%!
└── dev (5w)            # Different type than prod
    ├── team-a [memory=10GB]  # Absolute
    └── team-b (2w)           # Weight`}
                </pre>

                <Alert className="mt-4">
                  <AlertDescription className="text-xs">
                    In flexible mode, you can mix capacity types and don't need to sum to 100%. This
                    allows for more dynamic resource allocation based on actual usage.
                  </AlertDescription>
                </Alert>
              </div>
            </TabsContent>

            <TabsContent value="migration" className="space-y-4 pr-4">
              <h3 className="text-lg font-semibold">Migration Guide</h3>
              <p className="text-sm text-muted-foreground">
                Steps to migrate from Legacy Mode to Flexible Mode safely.
              </p>

              <div className="space-y-4">
                <Alert>
                  <AlertDescription>
                    Before migrating, ensure your YARN cluster version supports flexible mode and
                    test thoroughly in a non-production environment.
                  </AlertDescription>
                </Alert>

                <div className="space-y-3">
                  <h4 className="font-medium">Migration Steps</h4>

                  <div className="space-y-2">
                    <div className="flex items-start gap-2">
                      <Badge className="w-6 h-6 rounded-full p-0 flex items-center justify-center">
                        1
                      </Badge>
                      <div className="flex-1">
                        <p className="text-sm font-medium">Review Current Configuration</p>
                        <p className="text-xs text-muted-foreground">
                          Use the validation preview to see which errors would be removed when
                          switching to flexible mode.
                        </p>
                      </div>
                    </div>

                    <div className="flex items-start gap-2">
                      <Badge className="w-6 h-6 rounded-full p-0 flex items-center justify-center">
                        2
                      </Badge>
                      <div className="flex-1">
                        <p className="text-sm font-medium">Plan Capacity Changes</p>
                        <p className="text-xs text-muted-foreground">
                          Decide if you want to keep the same effective capacity distribution or
                          take advantage of flexible allocation.
                        </p>
                      </div>
                    </div>

                    <div className="flex items-start gap-2">
                      <Badge className="w-6 h-6 rounded-full p-0 flex items-center justify-center">
                        3
                      </Badge>
                      <div className="flex-1">
                        <p className="text-sm font-medium">Stage Changes</p>
                        <p className="text-xs text-muted-foreground">
                          Make necessary adjustments to queue capacities. In flexible mode, you can
                          stage changes that would be invalid in legacy mode.
                        </p>
                      </div>
                    </div>

                    <div className="flex items-start gap-2">
                      <Badge className="w-6 h-6 rounded-full p-0 flex items-center justify-center">
                        4
                      </Badge>
                      <div className="flex-1">
                        <p className="text-sm font-medium">Apply and Monitor</p>
                        <p className="text-xs text-muted-foreground">
                          Apply the configuration changes and monitor resource allocation to ensure
                          it meets your requirements.
                        </p>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="border rounded-lg p-3 bg-muted/50 mt-4">
                  <h4 className="font-medium text-sm mb-2">Best Practices</h4>
                  <ul className="text-xs space-y-1 text-muted-foreground">
                    <li className="flex items-start gap-1">
                      <ArrowRight className="h-3 w-3 mt-0.5 flex-shrink-0" />
                      <span>Test in a development environment first</span>
                    </li>
                    <li className="flex items-start gap-1">
                      <ArrowRight className="h-3 w-3 mt-0.5 flex-shrink-0" />
                      <span>Document your capacity allocation strategy</span>
                    </li>
                    <li className="flex items-start gap-1">
                      <ArrowRight className="h-3 w-3 mt-0.5 flex-shrink-0" />
                      <span>Consider using weights for dynamic workloads</span>
                    </li>
                    <li className="flex items-start gap-1">
                      <ArrowRight className="h-3 w-3 mt-0.5 flex-shrink-0" />
                      <span>Monitor queue utilization after migration</span>
                    </li>
                  </ul>
                </div>
              </div>
            </TabsContent>
          </div>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
};
