package org.apache.calcite.rex;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlStdOperatorTablePlus;


import java.util.Collections;
import java.util.List;

public class RexShuttlePlus extends RexShuttle {

  private final RexBuilderPlus rexBuilder;

  public RexShuttlePlus(RexBuilderPlus rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  @Override public RexNode visitCall(final RexCall call) {

    if (call.getOperator() == SqlStdOperatorTablePlus.LEN || call.getOperator() == SqlStdOperatorTablePlus.CHAR_LEN) {
      // Replace
      List<RexNode> operands = call.getOperands();
      if(operands == SqlStdOperatorTablePlus.CHAR_LEN){
        Collections.swap(operands, 0, 1);
      }
      return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, operands);
    }

    // Return unchanged
    else {
      return super.visitCall(call);
    }

  }

}
