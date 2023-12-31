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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTruncate;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Parse tree for {@code TRUNCATE TABLE} statement.
 */
public class SqlTruncateTable extends SqlTruncate {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.TRUNCATE_TABLE);
  public final SqlIdentifier name;
  public final boolean continueIdentify;

  /**
   * Creates a SqlTruncateTable.
   */
  public SqlTruncateTable(SqlParserPos pos, SqlIdentifier name, boolean continueIdentify) {
    super(OPERATOR, pos);
    this.name = name;
    this.continueIdentify = continueIdentify;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TRUNCATE");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (continueIdentify) {
      writer.keyword("CONTINUE IDENTITY");
    } else {
      writer.keyword("RESTART IDENTITY");

    }
  }
}
