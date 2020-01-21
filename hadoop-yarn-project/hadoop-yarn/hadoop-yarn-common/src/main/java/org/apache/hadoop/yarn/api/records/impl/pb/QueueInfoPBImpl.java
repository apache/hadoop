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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProto;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class QueueInfoPBImpl extends QueueInfo {

  QueueInfoProto proto = QueueInfoProto.getDefaultInstance();
  QueueInfoProto.Builder builder = null;
  boolean viaProto = false;

  List<ApplicationReport> applicationsList;
  List<QueueInfo> childQueuesList;
  Set<String> accessibleNodeLabels;
  Map<String, QueueConfigurations> queueConfigurations;

  public QueueInfoPBImpl() {
    builder = QueueInfoProto.newBuilder();
  }
  
  public QueueInfoPBImpl(QueueInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ApplicationReport> getApplications() {
    initLocalApplicationsList();
    return this.applicationsList;
  }

  @Override
  public float getCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapacity()) ? p.getCapacity() : -1;
  }

  @Override
  public List<QueueInfo> getChildQueues() {
    initLocalChildQueuesList();
    return this.childQueuesList;
  }

  @Override
  public float getCurrentCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCurrentCapacity()) ? p.getCurrentCapacity() : 0;
  }

  @Override
  public float getMaximumCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasMaximumCapacity()) ? p.getMaximumCapacity() : -1;
  }

  @Override
  public String getQueueName() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public QueueState getQueueState() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setApplications(List<ApplicationReport> applications) {
    if (applications == null) {
      builder.clearApplications();
    }
    this.applicationsList = applications;
  }

  @Override
  public void setCapacity(float capacity) {
    maybeInitBuilder();
    builder.setCapacity(capacity);
  }

  @Override
  public void setChildQueues(List<QueueInfo> childQueues) {
    if (childQueues == null) {
      builder.clearChildQueues();
    }
    this.childQueuesList = childQueues;
  }

  @Override
  public void setCurrentCapacity(float currentCapacity) {
    maybeInitBuilder();
    builder.setCurrentCapacity(currentCapacity);
  }

  @Override
  public void setMaximumCapacity(float maximumCapacity) {
    maybeInitBuilder();
    builder.setMaximumCapacity(maximumCapacity);
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName(queueName);
  }

  @Override
  public void setQueueState(QueueState queueState) {
    maybeInitBuilder();
    if (queueState == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(queueState));
  }

  public QueueInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void initLocalApplicationsList() {
    if (this.applicationsList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationReportProto> list = p.getApplicationsList();
    applicationsList = new ArrayList<ApplicationReport>();

    for (ApplicationReportProto a : list) {
      applicationsList.add(convertFromProtoFormat(a));
    }
  }

  private void addApplicationsToProto() {
    maybeInitBuilder();
    builder.clearApplications();
    if (applicationsList == null)
      return;
    Iterable<ApplicationReportProto> iterable = new Iterable<ApplicationReportProto>() {
      @Override
      public Iterator<ApplicationReportProto> iterator() {
        return new Iterator<ApplicationReportProto>() {
  
          Iterator<ApplicationReport> iter = applicationsList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public ApplicationReportProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllApplications(iterable);
  }

  private void initLocalChildQueuesList() {
    if (this.childQueuesList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueInfoProto> list = p.getChildQueuesList();
    childQueuesList = new ArrayList<QueueInfo>();

    for (QueueInfoProto a : list) {
      childQueuesList.add(convertFromProtoFormat(a));
    }
  }

  private void addChildQueuesInfoToProto() {
    maybeInitBuilder();
    builder.clearChildQueues();
    if (childQueuesList == null)
      return;
    Iterable<QueueInfoProto> iterable = new Iterable<QueueInfoProto>() {
      @Override
      public Iterator<QueueInfoProto> iterator() {
        return new Iterator<QueueInfoProto>() {
  
          Iterator<QueueInfo> iter = childQueuesList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public QueueInfoProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllChildQueues(iterable);
  }

  private void addQueueConfigurations() {
    maybeInitBuilder();
    builder.clearQueueConfigurationsMap();
    if (queueConfigurations == null) {
      return;
    }
    Iterable<? extends QueueConfigurationsMapProto> values =
        new Iterable<QueueConfigurationsMapProto>() {

      @Override
      public Iterator<QueueConfigurationsMapProto> iterator() {
        return new Iterator<QueueConfigurationsMapProto>() {
          private Iterator<String> iterator =
              queueConfigurations.keySet().iterator();

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public QueueConfigurationsMapProto next() {
            String key = iterator.next();
            return QueueConfigurationsMapProto.newBuilder()
                .setPartitionName(key)
                .setQueueConfigurations(
                    convertToProtoFormat(queueConfigurations.get(key)))
                .build();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    this.builder.addAllQueueConfigurationsMap(values);
  }

  private void mergeLocalToBuilder() {
    if (this.childQueuesList != null) {
      addChildQueuesInfoToProto();
    }
    if (this.applicationsList != null) {
      addApplicationsToProto();
    }
    if (this.accessibleNodeLabels != null) {
      builder.clearAccessibleNodeLabels();
      builder.addAllAccessibleNodeLabels(this.accessibleNodeLabels);
    }
    if (this.queueConfigurations != null) {
      addQueueConfigurations();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }


  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto a) {
    return new ApplicationReportPBImpl(a);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }

  private QueueInfoPBImpl convertFromProtoFormat(QueueInfoProto a) {
    return new QueueInfoPBImpl(a);
  }
  
  private QueueInfoProto convertToProtoFormat(QueueInfo q) {
    return ((QueueInfoPBImpl)q).getProto();
  }

  private QueueState convertFromProtoFormat(QueueStateProto q) {
    return ProtoUtils.convertFromProtoFormat(q);
  }

  private QueueStateProto convertToProtoFormat(QueueState queueState) {
    return ProtoUtils.convertToProtoFormat(queueState);
  }

  private QueueConfigurationsPBImpl convertFromProtoFormat(
      QueueConfigurationsProto q) {
    return new QueueConfigurationsPBImpl(q);
  }

  private QueueConfigurationsProto convertToProtoFormat(
      QueueConfigurations q) {
    return ((QueueConfigurationsPBImpl)q).getProto();
  }

  @Override
  public void setAccessibleNodeLabels(Set<String> nodeLabels) {
    maybeInitBuilder();
    builder.clearAccessibleNodeLabels();
    this.accessibleNodeLabels = nodeLabels;
  }
  
  private void initNodeLabels() {
    if (this.accessibleNodeLabels != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    this.accessibleNodeLabels = new HashSet<String>();
    this.accessibleNodeLabels.addAll(p.getAccessibleNodeLabelsList());
  }

  @Override
  public Set<String> getAccessibleNodeLabels() {
    initNodeLabels();
    return this.accessibleNodeLabels;
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasDefaultNodeLabelExpression()) ? p
        .getDefaultNodeLabelExpression().trim() : null;
  }

  @Override
  public void setDefaultNodeLabelExpression(String defaultNodeLabelExpression) {
    maybeInitBuilder();
    if (defaultNodeLabelExpression == null) {
      builder.clearDefaultNodeLabelExpression();
      return;
    }
    builder.setDefaultNodeLabelExpression(defaultNodeLabelExpression);
  }

  private QueueStatistics convertFromProtoFormat(QueueStatisticsProto q) {
    return new QueueStatisticsPBImpl(q);
  }

  private QueueStatisticsProto convertToProtoFormat(QueueStatistics q) {
    return ((QueueStatisticsPBImpl) q).getProto();
  }

  @Override
  public QueueStatistics getQueueStatistics() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueStatistics()) ? convertFromProtoFormat(p
      .getQueueStatistics()) : null;
  }

  @Override
  public void setQueueStatistics(QueueStatistics queueStatistics) {
    maybeInitBuilder();
    if (queueStatistics == null) {
      builder.clearQueueStatistics();
      return;
    }
    builder.setQueueStatistics(convertToProtoFormat(queueStatistics));
  }

  @Override
  public Boolean getPreemptionDisabled() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPreemptionDisabled()) ? p
        .getPreemptionDisabled() : null;
  }

  @Override
  public void setPreemptionDisabled(boolean preemptionDisabled) {
    maybeInitBuilder();
    builder.setPreemptionDisabled(preemptionDisabled);
  }

  private void initQueueConfigurations() {
    if (queueConfigurations != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueConfigurationsMapProto> lists = p.getQueueConfigurationsMapList();
    queueConfigurations =
        new HashMap<String, QueueConfigurations>(lists.size());
    for (QueueConfigurationsMapProto queueConfigurationsProto : lists) {
      queueConfigurations.put(queueConfigurationsProto.getPartitionName(),
          convertFromProtoFormat(
              queueConfigurationsProto.getQueueConfigurations()));
    }
  }

  @Override
  public Map<String, QueueConfigurations> getQueueConfigurations() {
    initQueueConfigurations();
    return queueConfigurations;
  }

  @Override
  public void setQueueConfigurations(
      Map<String, QueueConfigurations> queueConfigurations) {
    if (queueConfigurations == null) {
      return;
    }
    initQueueConfigurations();
    this.queueConfigurations.clear();
    this.queueConfigurations.putAll(queueConfigurations);
  }

  @Override
  public Boolean getIntraQueuePreemptionDisabled() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasIntraQueuePreemptionDisabled()) ? p
        .getIntraQueuePreemptionDisabled() : null;
  }

  @Override
  public void setIntraQueuePreemptionDisabled(
      boolean intraQueuePreemptionDisabled) {
    maybeInitBuilder();
    builder.setIntraQueuePreemptionDisabled(intraQueuePreemptionDisabled);
  }
}
