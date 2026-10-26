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


import type {
  PropertyCondition,
  PropertyDescriptor,
  PropertyEvaluationContext,
  PropertyEvaluationScope,
} from '~/types/property-descriptor';

export type PropertyConditionOptions = Omit<
  PropertyEvaluationContext,
  'property' | 'propertyValue' | 'scope'
> & {
  scope: PropertyEvaluationScope;
  property: PropertyDescriptor;
  propertyValue: string
};

function buildContext(options: PropertyConditionOptions): PropertyEvaluationContext {
  const { property, propertyValue, scope, ...rest } = options;
  return {
    property,
    propertyValue,
    scope,
    ...rest,
  };
}

function evaluate(
  conditions: PropertyCondition[] | undefined,
  options: PropertyConditionOptions,
): boolean {
  if (!conditions || conditions.length === 0) {
    return true;
  }

  const context = buildContext(options);

  return conditions.every((condition) => {
    try {
      return condition(context);
    } catch (error) {
      console.error('Failed to evaluate property condition', {
        property: options.property.name,
        scope: options.scope,
        error,
      });
      return true;
    }
  });
}

export function shouldShowProperty(
  property: PropertyDescriptor,
  options: PropertyConditionOptions,
): boolean {
  return evaluate(property.showWhen, options);
}

export function isPropertyEnabled(
  property: PropertyDescriptor,
  options: PropertyConditionOptions,
): boolean {
  return evaluate(property.enableWhen, options);
}
