package org.apache.calcite.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.OperandTypesPlus;
import org.apache.calcite.sql.type.ReturnTypes;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class SqlStdOperatorTablePlus extends SqlStdOperatorTable {

  private static @MonotonicNonNull SqlStdOperatorTablePlus instance;


  /**
   * Returns the standard operator table, creating it if necessary.
   */
  public static synchronized SqlStdOperatorTablePlus instance() {
    if (instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new SqlStdOperatorTablePlus();
      instance.init();
    }
    return instance;
  }



  public static final SqlFunction ABS_CUSTOM =
      new SqlFunction(
          "ABS_CUSTOM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.NUMERIC_OR_INTERVAL,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction SQRT_CUSTOM =
      new SqlFunction(
          "SQRT_CUSTOM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction POWER_CUSTOM =
      new SqlFunction(
          "POWER_CUSTOM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction STR_TO_INT =
      new SqlFunction(
          "STR_TO_INT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);



  public static final SqlFunction ROUND_CUSTOM =
      new SqlFunction(
          "ROUND_CUSTOM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.NUMERIC_OPTIONAL_INTEGER,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction DATETIME =
      new SqlFunction(
          "DATETIME",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE,
          null,
          OperandTypesPlus.DATETIME_STRING,
          SqlFunctionCategory.TIMEDATE);

  public static final SqlFunction CONVERT_TIMEZONE =
      new SqlFunction(
          "CONVERT_TIMEZONE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE,
          null,
          OperandTypesPlus.STRING_DATETIME,
          SqlFunctionCategory.TIMEDATE);



  public static final SqlFunction ADD =
      new SqlFunction(
          "ADD",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction PLUS =
      new SqlFunction(
          "PLUS",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);



  public static final SqlFunction SUM =
      new SqlFunction(
          "SUM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);


  public static final SqlFunction MINUS =
      new SqlFunction(
          "MINUS",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);


  public static final SqlFunction SUBTRACT =
      new SqlFunction(
          "SUBTRACT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);


  public static final SqlFunction LEN =  new Len();

  public static final SqlFunction CHAR_LEN =  new CharLen();





}
