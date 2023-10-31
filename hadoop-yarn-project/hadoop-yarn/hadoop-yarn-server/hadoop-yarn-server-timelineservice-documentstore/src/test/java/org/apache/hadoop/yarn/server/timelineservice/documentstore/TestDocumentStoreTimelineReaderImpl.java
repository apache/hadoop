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

package org.apache.hadoop.yarn.server.timelineservice.documentstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DummyDocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;


/**
 * Test case for {@link DocumentStoreTimelineReaderImpl}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DocumentStoreFactory.class)
public class TestDocumentStoreTimelineReaderImpl {

  private final DocumentStoreReader<TimelineDocument> documentStoreReader = new
      DummyDocumentStoreReader<>();
  private final List<TimelineEntity> entities = DocumentStoreTestUtils
      .bakeTimelineEntities();
  private final TimelineEntityDocument appTimelineEntity =
      DocumentStoreTestUtils.bakeTimelineEntityDoc();

  private final Configuration conf = new Configuration();
  private final TimelineReaderContext context = new
      TimelineReaderContext(null, null, null,
      1L, null, null, null);
  private final DocumentStoreTimelineReaderImpl timelineReader = new
      DocumentStoreTimelineReaderImpl();

  public TestDocumentStoreTimelineReaderImpl() throws IOException {
  }

  @Before
  public void setUp() throws YarnException {
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME,
        "TestDB");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_ENDPOINT,
        "https://localhost:443");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_MASTER_KEY,
        "1234567");
    PowerMockito.mockStatic(DocumentStoreFactory.class);
    PowerMockito.when(DocumentStoreFactory.createDocumentStoreReader(
        ArgumentMatchers.any(Configuration.class)))
        .thenReturn(documentStoreReader);
  }

  @Test(expected = YarnException.class)
  public void testFailOnNoCosmosDBConfigs() throws Exception {
    DocumentStoreUtils.validateCosmosDBConf(new Configuration());
  }

  @Test
  public void testGetEntity() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    EnumSet<TimelineReader.Field> fieldsToRetrieve = EnumSet.noneOf(
        TimelineReader.Field.class);
    dataToRetrieve.setFieldsToRetrieve(fieldsToRetrieve);

    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(appTimelineEntity.getCreatedTime(), timelineEntity
        .getCreatedTime().longValue());
    Assert.assertEquals(0, timelineEntity .getMetrics().size());
    Assert.assertEquals(0, timelineEntity.getEvents().size());
    Assert.assertEquals(0, timelineEntity.getConfigs().size());
    Assert.assertEquals(appTimelineEntity.getInfo().size(),
        timelineEntity.getInfo().size());
  }

  @Test
  public void testGetEntityCustomField() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.METRICS);

    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(appTimelineEntity.getCreatedTime(), timelineEntity
        .getCreatedTime().longValue());
    Assert.assertEquals(appTimelineEntity.getMetrics().size(),
        timelineEntity.getMetrics().size());
    Assert.assertEquals(0, timelineEntity.getEvents().size());
    Assert.assertEquals(0, timelineEntity.getConfigs().size());
    Assert.assertEquals(appTimelineEntity.getInfo().size(),
        timelineEntity.getInfo().size());
  }

  @Test
  public void testGetEntityAllFields() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.ALL);

    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(appTimelineEntity.getCreatedTime(), timelineEntity
        .getCreatedTime().longValue());
    Assert.assertEquals(appTimelineEntity.getMetrics().size(),
        timelineEntity .getMetrics().size());
    Assert.assertEquals(appTimelineEntity.getEvents().size(),
        timelineEntity.getEvents().size());
    Assert.assertEquals(appTimelineEntity.getConfigs().size(),
        timelineEntity.getConfigs().size());
    Assert.assertEquals(appTimelineEntity.getInfo().size(),
        timelineEntity.getInfo().size());
  }

  @Test
  public void testGetAllEntities() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.ALL);

    Set<TimelineEntity> actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().build(), dataToRetrieve);

    Assert.assertEquals(entities.size(), actualEntities.size());
  }

  @Test
  public void testGetEntitiesWithLimit() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();

    Set<TimelineEntity> actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().entityLimit(2L).build(),
        dataToRetrieve);

    Assert.assertEquals(2, actualEntities.size());
  }

  @Test
  public void testGetEntitiesByWindows() throws Exception {
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();

    Set<TimelineEntity> actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().createdTimeBegin(1533985554927L)
            .createTimeEnd(1533985554927L).build(), dataToRetrieve);

    Assert.assertEquals(1, actualEntities.size());
  }

  @Test
  public void testGetFilteredEntities() throws Exception {

    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    timelineReader.serviceInit(conf);
    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.ALL);

    // Get entities based on info filters.
    TimelineFilterList infoFilterList = new TimelineFilterList();
    infoFilterList.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL,
            "YARN_APPLICATION_ATTEMPT_FINAL_STATUS", "SUCCEEDED"));
    Set<TimelineEntity> actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList).build(),
        dataToRetrieve);

    Assert.assertEquals(1, actualEntities.size());
    // Only one entity with type YARN_APPLICATION_ATTEMPT should be returned.
    for (TimelineEntity entity : actualEntities) {
      if (!entity.getType().equals("YARN_APPLICATION_ATTEMPT")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    // Get entities based on config filters.
    TimelineFilterList confFilterList = new TimelineFilterList();
    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    confFilterList.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL,
            "YARN_AM_NODE_LABEL_EXPRESSION", "<DEFAULT_PARTITION>"));
    actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().configFilters(confFilterList)
            .build(), dataToRetrieve);

    Assert.assertEquals(1, actualEntities.size());
    // Only one entity with type YARN_APPLICATION should be returned.
    for (TimelineEntity entity : actualEntities) {
      if (!entity.getType().equals("YARN_APPLICATION")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    // Get entities based on event filters.
    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    TimelineFilterList eventFilters = new TimelineFilterList();
    eventFilters.addFilter(
        new TimelineExistsFilter(TimelineCompareOp.EQUAL,
            "CONTAINER_LAUNCHED"));
    actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().eventFilters(eventFilters).build(),
        dataToRetrieve);

    Assert.assertEquals(1, actualEntities.size());
    // Only one entity with type YARN_CONTAINER should be returned.
    for (TimelineEntity entity : actualEntities) {
      if (!entity.getType().equals("YARN_CONTAINER")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    // Get entities based on metric filters.
    TimelineFilterList metricFilterList = new TimelineFilterList();
    metricFilterList.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "MEMORY", 150298624L));
    actualEntities = timelineReader.getEntities(context,
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList)
            .build(), dataToRetrieve);

    Assert.assertEquals(1, actualEntities.size());
    // Only one entity with type YARN_CONTAINER should be returned.
    for (TimelineEntity entity : actualEntities) {
      if (!entity.getType().equals("YARN_CONTAINER")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }
  }

  @Test
  public void testReadingDifferentEntityTypes() throws Exception {

    timelineReader.serviceInit(conf);

    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();

    // reading YARN_FLOW_ACTIVITY
    context.setEntityType(TimelineEntityType.YARN_FLOW_ACTIVITY.toString());
    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(TimelineEntityType.YARN_FLOW_ACTIVITY.toString(),
        timelineEntity.getType());

    // reading YARN_FLOW_RUN
    context.setEntityType(TimelineEntityType.YARN_FLOW_RUN.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(TimelineEntityType.YARN_FLOW_RUN.toString(),
        timelineEntity.getType());

    // reading YARN_APPLICATION
    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);

    Assert.assertEquals(TimelineEntityType.YARN_APPLICATION.toString(),
        timelineEntity.getType());
  }

  @Test
  public void testReadingAllEntityTypes() throws Exception {

    timelineReader.serviceInit(conf);

    context.setEntityType(TimelineEntityType.YARN_CONTAINER.toString());
    Set<String> entityTypes = timelineReader.getEntityTypes(context);
    Assert.assertTrue(entityTypes.contains(TimelineEntityType.YARN_CONTAINER
        .toString()));
    Assert.assertTrue(entityTypes.contains(TimelineEntityType
        .YARN_APPLICATION_ATTEMPT.toString()));
  }

  @Test
  public void testMetricsToRetrieve() throws Exception {

    timelineReader.serviceInit(conf);

    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.METRICS);
    TimelineFilterList timelineFilterList = new TimelineFilterList();

    //testing metrics prefix for OR condition
    timelineFilterList.setOperator(TimelineFilterList.Operator.OR);
    timelineFilterList.addFilter(new TimelinePrefixFilter(
        TimelineCompareOp.EQUAL, "NOTHING"));
    dataToRetrieve.setMetricsToRetrieve(timelineFilterList);

    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertEquals(0, timelineEntity.getMetrics().size());

    timelineFilterList.addFilter(new TimelinePrefixFilter(
        TimelineCompareOp.EQUAL,
            "YARN_APPLICATION_NON_AM_CONTAINER_PREEMPTED"));
    dataToRetrieve.setMetricsToRetrieve(timelineFilterList);

    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertTrue(timelineEntity.getMetrics().size() > 0);

    //testing metrics prefix for AND condition
    timelineFilterList.setOperator(TimelineFilterList.Operator.AND);
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertEquals(0, timelineEntity.getMetrics().size());

    dataToRetrieve.getMetricsToRetrieve().getFilterList().remove(0);
    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertTrue(timelineEntity.getMetrics().size() > 0);
  }

  @Test
  public void testConfigsToRetrieve() throws Exception {

    timelineReader.serviceInit(conf);

    TimelineDataToRetrieve dataToRetrieve = new TimelineDataToRetrieve();
    dataToRetrieve.getFieldsToRetrieve().add(TimelineReader.Field.CONFIGS);
    TimelineFilterList timelineFilterList = new TimelineFilterList();

    //testing metrics prefix for OR condition
    timelineFilterList.setOperator(TimelineFilterList.Operator.OR);
    timelineFilterList.addFilter(new TimelinePrefixFilter(
        TimelineCompareOp.EQUAL, "NOTHING"));
    dataToRetrieve.setConfsToRetrieve(timelineFilterList);

    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    TimelineEntity timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertEquals(0, timelineEntity.getConfigs().size());

    timelineFilterList.addFilter(new TimelinePrefixFilter(
        TimelineCompareOp.EQUAL, "YARN_AM_NODE_LABEL_EXPRESSION"));
    dataToRetrieve.setConfsToRetrieve(timelineFilterList);

    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertTrue(timelineEntity.getConfigs().size() > 0);

    //testing metrics prefix for AND condition
    timelineFilterList.setOperator(TimelineFilterList.Operator.AND);
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertEquals(0, timelineEntity.getConfigs().size());

    dataToRetrieve.getConfsToRetrieve().getFilterList().remove(0);
    context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
    timelineEntity = timelineReader.getEntity(context,
        dataToRetrieve);
    Assert.assertTrue(timelineEntity.getConfigs().size() > 0);
  }
}