package org.apache.calcite.sql.parser;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;

public class QueryExecutionNode {
    public QueryExecutionNode(String sQueryId, String schemaName, CalciteSchema schema, String query, RelNode optimizedPhase2, RexBuilder rexBuilder, RelOptPlanner planner, RelMetadataQuery relMetaDataQuery) {
    }
}
