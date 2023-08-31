package org.apache.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class SchemaCustom extends AbstractSchema {

  private final ImmutableMap<String, Table> tables;

  public SchemaCustom() {
    tables = ImmutableMap.<String, Table>builder()
        .put("employees",
            new PkClusteredTable(
                factory ->
                    new RelDataTypeFactory.Builder(factory)
                        .add("empid", factory.createJavaType(int.class))
                        .add("name", factory.createJavaType(String.class))
                        .add("age", factory.createJavaType(int.class))
                        .build(),
                ImmutableBitSet.of(0),
                Arrays.asList(
                    new Object[]{1, "Alice", 30},
                    new Object[]{2, "Bob", 25},
                    new Object[]{3, "Charlie", 28})))
        .build();
  }

  public static SchemaCustom createRootSchema(boolean addMetadataSchema) {
    return createRootSchema(addMetadataSchema);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tables;
  }

  public String getSchemaName() {
      return "SchemaCustom";
  }

  private static class PkClusteredTable extends AbstractTable implements ScannableTable {
    private final ImmutableBitSet pkColumns;
    private final List<Object[]> data;
    private final Function<RelDataTypeFactory, RelDataType> typeBuilder;

    PkClusteredTable(
        Function<RelDataTypeFactory, RelDataType> dataTypeBuilder,
        ImmutableBitSet pkColumns,
        List<Object[]> data) {
      this.data = data;
      this.typeBuilder = dataTypeBuilder;
      this.pkColumns = pkColumns;
    }

    @Override
    public Statistic getStatistic() {
      List<RelFieldCollation> collationFields = new ArrayList<>();
      for (Integer key : pkColumns) {
        collationFields.add(
            new RelFieldCollation(
                key,
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.LAST));
      }
      return Statistics.of(data.size(), ImmutableList.of(pkColumns),
          ImmutableList.of(RelCollations.of(collationFields)));
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return typeBuilder.apply(typeFactory);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(final DataContext root) {
      return Linq4j.asEnumerable(data);
    }

  }
}
