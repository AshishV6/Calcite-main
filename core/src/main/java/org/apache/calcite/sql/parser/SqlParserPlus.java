package org.apache.calcite.sql.parser;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;

public abstract class SqlParserPlus
{

  protected abstract CalciteSchema getSchema();

  public abstract RexBuilder getRexBuilder();

  public abstract RelNode getOptimizedRelNode(SqlNode sqlNode);

  public abstract RelOptPlanner getPlanner();

  public abstract RelMetadataQuery getRelMetaDataQuery();



  protected abstract SqlNode parseX(String query) throws SqlParseException;


  public QueryExecutionNode parse(String sQueryId, String catalogName, String schemaName, String query)
  {
    try
    {
      SqlNode sqlNode = parseX(query);
      SqlKind sqlKind = sqlNode.getKind();
      if (!isDdl(sqlKind))
      {
        RelNode optimizedPhase2 = getOptimizedRelNode(sqlNode);
        return new QueryExecutionNode(sQueryId, schemaName, getSchema(), query, optimizedPhase2, getRexBuilder(),
            getPlanner(), getRelMetaDataQuery());
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
    return null;
  }

  protected boolean isDdl(SqlKind sqlKind)
  {
    switch (sqlKind)
    {
    case SELECT:
    case INTERSECT:
    case VALUES:
    case WITH:
    case UNION:
    case EXCEPT:
    case INSERT:
    case DELETE:
    case UPDATE:
    case MERGE:
    case UNNEST:
    case OTHER_FUNCTION:
    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
    case ORDER_BY:
      return false;
    default:
      return true;
    }
  }

}
