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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Finds all the the target component specs.
 */
public interface UpgradeComponentsFinder {

  List<Component> findTargetComponentSpecs(Service currentDef,
      Service targetDef);

  /**
   * Default implementation of {@link UpgradeComponentsFinder} that finds all
   * the target component specs.
   */
  class DefaultUpgradeComponentsFinder implements UpgradeComponentsFinder {

    @Override
    public List<Component> findTargetComponentSpecs(Service currentDef,
        Service targetDef) {
      if (currentDef.getComponents().size() !=
          targetDef.getComponents().size()) {
        throw new UnsupportedOperationException(
            "addition/deletion of components not supported by upgrade");
      }
      if (!currentDef.getKerberosPrincipal().equals(
          targetDef.getKerberosPrincipal())) {
        throw new UnsupportedOperationException("changes to kerberos " +
            "principal not supported by upgrade");
      }
      if (!Objects.equals(currentDef.getQueue(), targetDef.getQueue())) {
        throw new UnsupportedOperationException("changes to queue " +
            "not supported by upgrade");
      }

      if (!Objects.equals(currentDef.getResource(), targetDef.getResource())) {
        throw new UnsupportedOperationException("changes to resource " +
            "not supported by upgrade");
      }

      if (!Objects.equals(currentDef.getDescription(),
          targetDef.getDescription())) {
        throw new UnsupportedOperationException("changes to description " +
            "not supported by upgrade");
      }

      if (!Objects.equals(currentDef.getLaunchTime(),
          targetDef.getLaunchTime())) {
        throw new UnsupportedOperationException("changes to launch time " +
            "not supported by upgrade");
      }


      if (!Objects.equals(currentDef.getLifetime(),
          targetDef.getLifetime())) {
        throw new UnsupportedOperationException("changes to lifetime " +
            "not supported by upgrade");
      }

      if (!Objects.equals(currentDef.getConfiguration(),
          targetDef.getConfiguration())) {
        return targetDef.getComponents();
      }

      if (!Objects.equals(currentDef.getArtifact(), targetDef.getArtifact())) {
        return targetDef.getComponents();
      }

      List<Component> targetComps = new ArrayList<>();
      targetDef.getComponents().forEach(component -> {
        Component currentComp = currentDef.getComponent(component.getName());

        if (currentComp != null) {
          if (!Objects.equals(currentComp.getName(), component.getName())) {
            throw new UnsupportedOperationException(
                "changes to component name not supported by upgrade");
          }

          if (!Objects.equals(currentComp.getDependencies(),
              component.getDependencies())) {
            throw new UnsupportedOperationException(
                "changes to component dependencies not supported by upgrade");
          }

          if (!Objects.equals(currentComp.getReadinessCheck(),
              component.getReadinessCheck())) {
            throw new UnsupportedOperationException(
                "changes to component readiness check not supported by "
                    + "upgrade");
          }

          if (!Objects.equals(currentComp.getResource(),
              component.getResource())) {

            throw new UnsupportedOperationException(
                "changes to component resource not supported by upgrade");
          }

          if (!Objects.equals(currentComp.getRunPrivilegedContainer(),
              component.getRunPrivilegedContainer())) {
            throw new UnsupportedOperationException(
                "changes to run privileged container not supported by upgrade");
          }

          if (!Objects.equals(currentComp.getPlacementPolicy(),
              component.getPlacementPolicy())) {
            throw new UnsupportedOperationException(
                "changes to component placement policy not supported by "
                    + "upgrade");
          }

          if (!Objects.equals(currentComp.getQuicklinks(),
              component.getQuicklinks())) {
            throw new UnsupportedOperationException(
                "changes to component quick links not supported by upgrade");
          }

          if (!Objects.equals(currentComp.getArtifact(),
              component.getArtifact()) || !Objects.equals(
              currentComp.getLaunchCommand(), component.getLaunchCommand())
              || !Objects.equals(currentComp.getConfiguration(),
              component.getConfiguration())) {
            targetComps.add(component);
          }
        } else{
          throw new UnsupportedOperationException(
              "addition/deletion of components not supported by upgrade. "
                  + "Could not find component " + component.getName() + " in "
                  + "current service definition.");
        }
      });
      return targetComps;
    }
  }
}
