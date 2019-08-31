/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hadoop.ozone.recon.codegen;

import java.io.File;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.hadoop.ozone.recon.schema.ReconSchemaDefinition;
import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Database;
import org.jooq.meta.jaxb.Generate;
import org.jooq.meta.jaxb.Generator;
import org.jooq.meta.jaxb.Jdbc;
import org.jooq.meta.jaxb.Strategy;
import org.jooq.meta.jaxb.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteDataSource;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;

/**
 * Utility class that generates the Dao and Pojos for Recon schema. The
 * implementations of {@link ReconSchemaDefinition} are discovered through
 * Guice bindings in order to avoid ugly reflection code, and invoked to
 * generate the schema over an embedded database. The jooq code generator then
 * runs over the embedded database to generate classes for recon.
 */
public class JooqCodeGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(JooqCodeGenerator.class);

  private static final String SQLITE_DB =
      System.getProperty("java.io.tmpdir") + "/recon-generated-schema";
  private static final String JDBC_URL = "jdbc:sqlite:" + SQLITE_DB;

  private final Set<ReconSchemaDefinition> allDefinitions;

  @Inject
  public JooqCodeGenerator(Set<ReconSchemaDefinition> allDefinitions) {
    this.allDefinitions = allDefinitions;
  }

  /**
   * Create schema.
   */
  private void initializeSchema() throws SQLException {
    for (ReconSchemaDefinition definition : allDefinitions) {
      definition.initializeSchema();
    }
  }

  /**
   * Generate entity and DAO classes.
   */
  private void generateSourceCode(String outputDir) throws Exception {
    Configuration configuration =
        new Configuration()
            .withJdbc(new Jdbc()
                .withDriver("org.sqlite.JDBC")
                .withUrl(JDBC_URL)
                .withUser("sa")
                .withPassword("sa"))
            .withGenerator(new Generator()
                .withDatabase(new Database()
                    .withName("org.jooq.meta.sqlite.SQLiteDatabase")
                    .withOutputSchemaToDefault(true)
                    .withIncludeTables(true)
                    .withIncludePrimaryKeys(true))
                .withGenerate(new Generate()
                    .withDaos(true)
                    .withEmptyCatalogs(true)
                    .withEmptySchemas(true))
                .withStrategy(new Strategy().withName(
                    "org.hadoop.ozone.recon.codegen.TableNamingStrategy"))
                .withTarget(new Target()
                    .withPackageName("org.hadoop.ozone.recon.schema")
                    .withClean(true)
                    .withDirectory(outputDir)));
    GenerationTool.generate(configuration);
  }

  /**
   * Provider for embedded datasource.
   */
  static class LocalDataSourceProvider implements Provider<DataSource> {
    private static SQLiteDataSource db;

    static {
      db = new SQLiteDataSource();
      db.setUrl(JDBC_URL);
    }

    @Override
    public DataSource get() {
      return db;
    }

    static void cleanup() {
      FileUtils.deleteQuietly(new File(SQLITE_DB));
    }
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing required arguments: " +
          "Need a ouput directory for generated code.\nUsage: " +
          "org.apache.hadoop.ozone.recon.persistence.JooqCodeGenerator " +
          "<outputDirectory>.");
    }

    String outputDir = args[0];
    Injector injector = Guice.createInjector(
        new ReconSchemaGenerationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DataSource.class).toProvider(new LocalDataSourceProvider());
            bind(JooqCodeGenerator.class);
          }
        });

    JooqCodeGenerator codeGenerator =
        injector.getInstance(JooqCodeGenerator.class);

    // Create tables
    try {
      codeGenerator.initializeSchema();
    } catch (SQLException e) {
      LOG.error("Unable to initialize schema.", e);
      throw new ExceptionInInitializerError(e);
    }

    // Generate Pojos and Daos
    try {
      codeGenerator.generateSourceCode(outputDir);
    } catch (Exception e) {
      LOG.error("Code generation failed. Aborting build.", e);
      throw new ExceptionInInitializerError(e);
    }

    // Cleanup after
    LocalDataSourceProvider.cleanup();
  }
}
