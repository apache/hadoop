/*
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

package org.apache.hadoop.yarn.appcatalog.application;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.appcatalog.model.AppStoreEntry;
import org.apache.hadoop.yarn.appcatalog.model.Application;
import org.apache.hadoop.yarn.appcatalog.utils.RandomWord;
import org.apache.hadoop.yarn.appcatalog.utils.WordLengthException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for accessing Solr.
 */
public class AppCatalogSolrClient {

  private static final Logger LOG = LoggerFactory.getLogger(AppCatalogSolrClient.class);
  private static String urlString;

  public AppCatalogSolrClient() {
    // Locate Solr URL
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream input =
        classLoader.getResourceAsStream("appcatalog.properties");
    Properties properties = new Properties();
    try {
      properties.load(input);
      setSolrUrl(properties.getProperty("solr_url"));
    } catch (IOException e) {
      LOG.error("Error reading appcatalog configuration: ", e);
    }
  }

  private synchronized static void setSolrUrl(String url) {
    urlString = url;
  }

  public SolrClient getSolrClient() {
    return new HttpSolrClient.Builder(urlString).build();
  }

  public List<AppStoreEntry> getRecommendedApps() {
    List<AppStoreEntry> apps = new ArrayList<AppStoreEntry>();
    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.setSort("download_i", ORDER.desc);
    query.setFilterQueries("type_s:AppStoreEntry");
    query.setRows(40);
    QueryResponse response;
    try {
      response = solr.query(query);
      Iterator<SolrDocument> list = response.getResults().listIterator();
      while (list.hasNext()) {
        SolrDocument d = list.next();
        AppStoreEntry entry = new AppStoreEntry();
        entry.setId(d.get("id").toString());
        entry.setOrg(d.get("org_s").toString());
        entry.setName(d.get("name_s").toString());
        entry.setDesc(d.get("desc_s").toString());
        if (d.get("icon_s")!=null) {
          entry.setIcon(d.get("icon_s").toString());
        }
        entry.setLike(Integer.parseInt(d.get("like_i").toString()));
        entry.setDownload(Integer.parseInt(d.get("download_i").toString()));
        apps.add(entry);
      }
    } catch (SolrServerException | IOException e) {
      LOG.error("Error getting a list of recommended applications: ", e);
    }
    return apps;
  }

  public List<AppStoreEntry> search(String keyword) {
    List<AppStoreEntry> apps = new ArrayList<AppStoreEntry>();
    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    if (keyword.length()==0) {
      query.setQuery("*:*");
      query.setFilterQueries("type_s:AppStoreEntry");
    } else {
      query.setQuery(keyword);
      query.setFilterQueries("type_s:AppStoreEntry");
    }
    query.setRows(40);
    QueryResponse response;
    try {
      response = solr.query(query);
      Iterator<SolrDocument> list = response.getResults().listIterator();
      while (list.hasNext()) {
        SolrDocument d = list.next();
        AppStoreEntry entry = new AppStoreEntry();
        entry.setId(d.get("id").toString());
        entry.setOrg(d.get("org_s").toString());
        entry.setName(d.get("name_s").toString());
        entry.setDesc(d.get("desc_s").toString());
        entry.setLike(Integer.parseInt(d.get("like_i").toString()));
        entry.setDownload(Integer.parseInt(d.get("download_i").toString()));
        apps.add(entry);
      }
    } catch (SolrServerException | IOException e) {
      LOG.error("Error in searching for applications: ", e);
    }
    return apps;
  }

  public List<AppEntry> listAppEntries() {
    List<AppEntry> list = new ArrayList<AppEntry>();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.setFilterQueries("type_s:AppEntry");
    query.setRows(40);
    QueryResponse response;
    try {
      response = solr.query(query);
      Iterator<SolrDocument> appList = response.getResults().listIterator();
      while (appList.hasNext()) {
        SolrDocument d = appList.next();
        AppEntry entry = new AppEntry();
        entry.setId(d.get("id").toString());
        entry.setName(d.get("name_s").toString());
        entry.setApp(d.get("app_s").toString());
        entry.setYarnfile(mapper.readValue(d.get("yarnfile_s").toString(),
            Service.class));
        list.add(entry);
      }
    } catch (SolrServerException | IOException e) {
      LOG.error("Error in listing deployed applications: ", e);
    }
    return list;
  }

  public AppStoreEntry findAppStoreEntry(String id) {
    AppStoreEntry entry = new AppStoreEntry();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    query.setQuery("id:" + id);
    query.setFilterQueries("type_s:AppStoreEntry");
    query.setRows(1);

    QueryResponse response;
    try {
      response = solr.query(query);
      Iterator<SolrDocument> appList = response.getResults().listIterator();
      while (appList.hasNext()) {
        SolrDocument d = appList.next();
        entry.setId(d.get("id").toString());
        entry.setOrg(d.get("org_s").toString());
        entry.setName(d.get("name_s").toString());
        entry.setDesc(d.get("desc_s").toString());
        entry.setLike(Integer.parseInt(d.get("like_i").toString()));
        entry.setDownload(Integer.parseInt(d.get("download_i").toString()));
        Service yarnApp = mapper.readValue(d.get("yarnfile_s").toString(),
            Service.class);
        String name;
        try {
          Random r = new Random();
          int low = 3;
          int high = 10;
          int seed = r.nextInt(high-low) + low;
          int seed2 = r.nextInt(high-low) + low;
          name = RandomWord.getNewWord(seed).toLowerCase() + "-" + RandomWord
              .getNewWord(seed2).toLowerCase();
        } catch (WordLengthException e) {
          name = "c" + java.util.UUID.randomUUID().toString().substring(0, 11);
        }
        yarnApp.setName(name);
        entry.setApp(yarnApp);
      }
    } catch (SolrServerException | IOException e) {
      LOG.error("Error in finding deployed application: " + id, e);
    }
    return entry;
  }

  public AppEntry findAppEntry(String id) {
    AppEntry entry = new AppEntry();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    query.setQuery("id:" + id);
    query.setFilterQueries("type_s:AppEntry");
    query.setRows(1);

    QueryResponse response;
    try {
      response = solr.query(query);
      Iterator<SolrDocument> appList = response.getResults().listIterator();
      while (appList.hasNext()) {
        SolrDocument d = appList.next();
        entry.setId(d.get("id").toString());
        entry.setApp(d.get("app_s").toString());
        entry.setName(d.get("name_s").toString());
        entry.setYarnfile(mapper.readValue(d.get("yarnfile_s").toString(),
            Service.class));
      }
    } catch (SolrServerException | IOException e) {
      LOG.error("Error in finding deployed application: " + id, e);
    }
    return entry;
  }

  public void deployApp(String id, Service service) throws SolrServerException,
      IOException {
    long download = 0;
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
    SolrClient solr = getSolrClient();
    // Find application information from AppStore
    SolrQuery query = new SolrQuery();
    query.setQuery("id:" + id);
    query.setFilterQueries("type_s:AppStoreEntry");
    query.setRows(1);
    QueryResponse response = solr.query(query);
    Iterator<SolrDocument> appList = response.getResults().listIterator();
    AppStoreEntry entry = new AppStoreEntry();
    while (appList.hasNext()) {
      SolrDocument d = appList.next();
      entry.setOrg(d.get("org_s").toString());
      entry.setName(d.get("name_s").toString());
      entry.setDesc(d.get("desc_s").toString());
      entry.setLike(Integer.parseInt(d.get("like_i").toString()));
      entry.setDownload(Integer.parseInt(d.get("download_i").toString()));
      download = entry.getDownload() + 1;

      // Update download count
      docs.add(incrementDownload(d, download));
    }

    // increment download count for application

    if (service!=null) {
      // Register deployed application instance with AppList
      SolrInputDocument request = new SolrInputDocument();
      String name = service.getName();
      request.addField("type_s", "AppEntry");
      request.addField("id", name);
      request.addField("name_s", name);
      request.addField("app_s", entry.getOrg()+"/"+entry.getName());
      request.addField("yarnfile_s", mapper.writeValueAsString(service));
      docs.add(request);
    }

    try {
      commitSolrChanges(solr, docs);
    } catch (IOException e) {
      throw new IOException("Unable to register docker instance "
          + "with application entry.", e);
    }
  }

  private SolrInputDocument incrementDownload(SolrDocument doc,
      long download) {
    Collection<String> names = doc.getFieldNames();
    SolrInputDocument s = new SolrInputDocument();
    for (String name : names) {
      if(!name.equals("_version_")) {
        s.addField(name, doc.getFieldValues(name));
      }
    }
    download++;
    s.setField("download_i", download);
    return s;
  }

  public void deleteApp(String id) {
    SolrClient solr = getSolrClient();
    try {
      solr.deleteById(id);
      solr.commit();
    } catch (SolrServerException | IOException e) {
      LOG.error("Error in removing deployed application: "+id, e);
    }
  }

  public void register(Application app) throws IOException {
    Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
    SolrClient solr = getSolrClient();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      SolrInputDocument buffer = new SolrInputDocument();
      buffer.setField("id", java.util.UUID.randomUUID().toString()
          .substring(0, 11));
      buffer.setField("org_s", app.getOrganization());
      buffer.setField("name_s", app.getName());
      buffer.setField("desc_s", app.getDescription());
      if (app.getIcon() != null) {
        buffer.setField("icon_s", app.getIcon());
      }
      buffer.setField("type_s", "AppStoreEntry");
      buffer.setField("like_i", 0);
      buffer.setField("download_i", 0);

      // Keep only YARN data model for yarnfile field
      String yarnFile = mapper.writeValueAsString(app);
      LOG.info("app:"+yarnFile);
      Service yarnApp = mapper.readValue(yarnFile, Service.class);
      buffer.setField("yarnfile_s", mapper.writeValueAsString(yarnApp));

      docs.add(buffer);
      commitSolrChanges(solr, docs);
    } catch (SolrServerException | IOException e) {
      throw new IOException("Unable to register application " +
          "in Application Store. ", e);
    }
  }

  protected void register(AppStoreEntry app) throws IOException {
    Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
    SolrClient solr = getSolrClient();
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      SolrInputDocument buffer = new SolrInputDocument();
      buffer.setField("id", java.util.UUID.randomUUID().toString()
          .substring(0, 11));
      buffer.setField("org_s", app.getOrg());
      buffer.setField("name_s", app.getName());
      buffer.setField("desc_s", app.getDesc());
      if (app.getIcon() != null) {
        buffer.setField("icon_s", app.getIcon());
      }
      buffer.setField("type_s", "AppStoreEntry");
      buffer.setField("like_i", app.getLike());
      buffer.setField("download_i", app.getDownload());

      // Keep only YARN data model for yarnfile field
      String yarnFile = mapper.writeValueAsString(app);
      LOG.info("app:"+yarnFile);
      Service yarnApp = mapper.readValue(yarnFile, Service.class);
      buffer.setField("yarnfile_s", mapper.writeValueAsString(yarnApp));

      docs.add(buffer);
      commitSolrChanges(solr, docs);
    } catch (SolrServerException | IOException e) {
      throw new IOException("Unable to register application " +
          "in Application Store. ", e);
    }
  }

  public void upgradeApp(Service service) throws IOException,
      SolrServerException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
    SolrClient solr = getSolrClient();
    if (service!=null) {
      String name = service.getName();
      String app = "";
      SolrQuery query = new SolrQuery();
      query.setQuery("id:" + name);
      query.setFilterQueries("type_s:AppEntry");
      query.setRows(1);

      QueryResponse response;
      try {
        response = solr.query(query);
        Iterator<SolrDocument> appList = response.getResults().listIterator();
        while (appList.hasNext()) {
          SolrDocument d = appList.next();
          app = d.get("app_s").toString();
        }
      } catch (SolrServerException | IOException e) {
        LOG.error("Error in finding deployed application: " + name, e);
      }
      // Register deployed application instance with AppList
      SolrInputDocument request = new SolrInputDocument();
      request.addField("type_s", "AppEntry");
      request.addField("id", name);
      request.addField("name_s", name);
      request.addField("app_s", app);
      request.addField("yarnfile_s", mapper.writeValueAsString(service));
      docs.add(request);
    }
    try {
      commitSolrChanges(solr, docs);
    } catch (IOException e) {
      throw new IOException("Unable to register docker instance "
          + "with application entry.", e);
    }
  }

  private void commitSolrChanges(SolrClient solr,
      Collection<SolrInputDocument> docs)
          throws IOException, SolrServerException {
    // Commit Solr changes.
    UpdateResponse detailsResponse = solr.add(docs);
    if (detailsResponse.getStatus() != 0) {
      throw new IOException("Failed to commit document in solr, status code: "
          + detailsResponse.getStatus());
    }
    solr.commit();
  }
}
