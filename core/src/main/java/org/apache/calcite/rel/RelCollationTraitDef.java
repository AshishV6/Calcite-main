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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Definition of the ordering trait.
 *
 * <p>Ordering is a physical property (i.e. a trait) because it can be changed
 * without loss of information. The converter to do this is the
 * {@link org.apache.calcite.rel.core.Sort} operator.
 *
 * <p>Unlike other current traits, a {@link RelNode} can have more than one
 * value of this trait simultaneously. For example,
 * <code>LogicalTableScan(table=TIME_BY_DAY)</code> might be sorted by
 * <code>{the_year, the_month, the_date}</code> and also by
 * <code>{time_id}</code>. We have to allow a RelNode to belong to more than
 * one RelSubset (these RelSubsets are always in the same set).
 */
public class RelCollationTraitDef extends RelTraitDef<RelCollation> {
  public static final RelCollationTraitDef INSTANCE =
      new RelCollationTraitDef();

  private RelCollationTraitDef() {
  }

  @Override public Class<RelCollation> getTraitClass() {
    return RelCollation.class;
  }

  @Override public String getSimpleName() {
    return "sort";
  }

  @Override public boolean multiple() {
    return true;
  }

  @Override public RelCollation getDefault() {
    return RelCollations.EMPTY;
  }

  @Override public @Nullable RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      RelCollation toCollation,
      boolean allowInfiniteCostConverters) {
    if (toCollation.getFieldCollations().isEmpty()) {
      // An empty sort doesn't make sense.
      return null;
    }

    // Create a logical sort, then ask the planner to convert its remaining
    // traits (e.g. convert it to an EnumerableSortRel if rel is enumerable
    // convention)
    final Sort sort = LogicalSort.create(rel, toCollation, null, null);
    RelNode newRel = planner.register(sort, rel);
    final RelTraitSet newTraitSet = rel.getTraitSet().replace(toCollation);
    if (!newRel.getTraitSet().equals(newTraitSet)) {
      newRel = planner.changeTraits(newRel, newTraitSet);
    }
    return newRel;
  }

  @Override public boolean canConvert(
      RelOptPlanner planner, RelCollation fromTrait, RelCollation toTrait) {
    return true;
  }
}
