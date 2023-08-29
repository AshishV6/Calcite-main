package org.apache.calcite.rex;


import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import java.util.List;

public class RexBuilderPlus extends RexBuilder {
  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */
  public RexBuilderPlus(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override
  public RexNode makecall(RelDataType returnType,
      SqlOperator op,
      List<RexNode> exprs){

    RexCall rexCall = new RexCall(returnType, op, exprs);
    return rexCall;
  };



}
