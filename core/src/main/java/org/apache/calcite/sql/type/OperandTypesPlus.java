package org.apache.calcite.sql.type;

import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;


public abstract class OperandTypesPlus extends OperandTypes{

  public static final SqlSingleOperandTypeChecker DATETIME_STRING =
      family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker INTEGER_STRING =
      family(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER =
      family(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING);


  public static final SqlSingleOperandTypeChecker STRING_DATETIME =
      family(SqlTypeFamily.STRING, SqlTypeFamily.TIMESTAMP);

}
