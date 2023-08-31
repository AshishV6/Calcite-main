package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlStdOperatorTablePlus;

import java.util.Collections;
import java.util.List;


public class RexBuilderPlus extends RexBuilder {
  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */

  public RexCall call;
  public RexBuilderPlus rexBuilder;

  @Override
  public RexBuilderPlus getRexBuilder() {
    return rexBuilder;
  }

  public RexBuilderPlus(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override
  public final RexNode makeCall(
      SqlOperator op,
      List<? extends RexNode> exprs
  ) {

    if (call.getOperator() == SqlStdOperatorTablePlus.CHAR_LEN) {
      // Replace
      List<RexNode> operands = call.getOperands();
      if (operands == SqlStdOperatorTablePlus.CHAR_LEN) {
        Collections.swap(operands, 0, 1);
      }
      return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, operands);
    }
    return rexBuilder.makeCall(op, exprs);

  }
}
