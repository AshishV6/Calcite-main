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

package org.apache.calcite.sql;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.QueryCountMetrics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPlus;

import java.time.Duration;

public class SqlQueryPlan {
  public void getQueryPlan(String catalogName, String sSchemaName, String sQueryString)
  {
    //E6Schema schema = m_metadataService.getRootSchema();
    SqlParserPlus parser = getPlanner(schema, catalogName, sSchemaName, Quoting.DOUBLE_QUOTE);
    parser.parse("", catalogName, sSchemaName, sQueryString);

  }

  private SqlParserPlus getPlanner(CalciteSchema rootSchema, String defaultCatalog, String defaultSchema, Quoting quoting)
  {
    SqlParserPlus parser;
    parser = HepSqlPlanner.create(rootSchema, defaultCatalog, defaultSchema,
          new Timeout(Duration.ofSeconds(Env.HEP_PLANNER_TIMEOUT)), quoting);

    return parser;
  }


  public void parse(String sQueryId, String catalogName, String schemaName, String query)
  {
    try
    {
      SqlNode sqlNode = parseX(query);
      SqlKind sqlKind = sqlNode.getKind();
      if (!isDdl(sqlKind))
      {
        RelNode optimizedPhase2 = getOptimizedRelNode(sqlNode);
        System.out.println("Plan is : " + optimizedPhase2.explain());
      }

    }
    catch (SqlParseException ex)
    {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    catch (UnsupportedOperationException ex)
    {
      String message = ex.getMessage() == null ? "Unsupported operation in sql" : ex.getMessage();
      throw new RuntimeException(message, ex);
    }
    catch(Throwable ex)
    {
      String message = ex.getMessage() == null ? "Error parsing query" : ex.getMessage();
      throw new RuntimeException(message, ex);
    }
  }

  @Override
  public RelNode getOptimizedRelNode(SqlNode sqlNode)
  {
    RelNode converted = convert(sqlNode);
    RelNode optimized = optimize(converted);
    QueryCountMetrics.hep();
    return optimized;
  }
}
