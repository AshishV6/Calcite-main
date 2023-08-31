package org.apache.calcite.sql;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepSqlPlanner;
import org.apache.calcite.plan.hep.QueryCountMetrics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaCustom;
import org.apache.calcite.sql.parser.QueryExecutionNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPlus;

import java.time.Duration;

import static org.apache.calcite.plan.RelOptRule.convert;

public class SqlQueryPlan {
  public QueryExecutionNode getQueryPlan(String sSchemaName, String sQueryString)
  {
    SchemaCustom schema = new SchemaCustom();
    SqlParserPlus parser = getPlanner(schema, sSchemaName);
    QueryExecutionNode logPlan = parser.parse("", sSchemaName, sQueryString);

      return logPlan;
  }

  private SqlParserPlus getPlanner(SchemaCustom rootSchema, String defaultSchema)
  {
    SqlParserPlus parser;
    parser = HepSqlPlanner.create(rootSchema, defaultSchema);

    return parser;
  }


  public void parse(String sQueryId, String schemaName, String query)
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

    } catch (UnsupportedOperationException ex)
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

  private boolean isDdl(SqlKind sqlKind) {
      return false;
  }

  private SqlNode parseX(String query) {
      return null;
  }


  public RelNode getOptimizedRelNode(SqlNode sqlNode)
  {
    RelNode converted = convert(sqlNode);
    RelNode optimized = optimize(converted);
    QueryCountMetrics.hep();
    return optimized;
  }

  private RelNode optimize(RelNode converted) {
      return converted;
  }

  private RelNode convert(SqlNode sqlNode) {
      return null;
  }

  private class Timeout {
    public Timeout(Duration duration) {
    }
  }
}
