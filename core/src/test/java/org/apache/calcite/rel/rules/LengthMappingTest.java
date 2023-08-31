/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaCustom;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schemas.HrClusteredSchema;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPlus;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Util;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.apache.calcite.plan.hep.HepSqlPlanner.NOOP_EXPANDER;
import static org.apache.calcite.plan.volcano.PlannerTests.newCluster;
import static org.apache.calcite.sql.parser.SqlParserPlus.config;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the application of the {@link LengthMappingTest}.
 */
public final class LengthMappingTest {
  @Test void LengthMapping() throws Exception {
//    String sqlQuery = "SELECT Len(6,'ashish'), CharLen('ashish',6)";
//    SchemaCustom schema = new SchemaCustom();
//
//    SqlQueryPlan queryPlan = new SqlQueryPlan();
//
//    queryPlan.getQueryPlan(schema.getSchemaName(), sqlQuery);


    enum Type implements java.lang.reflect.Type {

      SIMPLE,
      ADVANCED,
      PUSHDOWN;

      @Override
      public String getTypeName() {
        return java.lang.reflect.Type.super.getTypeName();
      }
    }



    String sqlQuery = "SELECT Len(6,'ashish'), CharLen('ashish',6)";
    long start = System.currentTimeMillis();
    Enumerable<Objects> rows = execute(sqlQuery, Type.SIMPLE);
      for (Object row : rows) {
          if (row instanceof Object[]) {
              System.out.println(Arrays.toString((Object[]) row));
          } else {
              System.out.println(row);
          }
      }
      long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }


  public static <T> Enumerable<T> execute(String sqlQuery, Type processorType)
      throws SqlParseException {
    System.out.println("[Input query]");
    System.out.println(sqlQuery);
    System.out.println();


    // Create the schema and table data types
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();


    // Create an SQL parser
    CalciteConnectionConfig config;
    SqlParserPlus parser = SqlParserPlus.create(sqlQuery, config());
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();
    System.out.println("[Parsed query]");
    System.out.println(sqlNode);
    System.out.println();


    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList("bs"),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTablePlus.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster((VolcanoPlanner) typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());


    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    logPlan.explain();
    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));



    return null;





//    String sqlQuery = "SELECT Len('ashish',5), CharLen(5,'ashish')";
//    CalciteSchema schema = CalciteSchema.createRootSchema(true);
//
//    SqlQueryPlan queryPlanner = new SqlQueryPlan();
//    queryPlanner.getQueryPlan(schema.name, sqlQuery);



  }
}
