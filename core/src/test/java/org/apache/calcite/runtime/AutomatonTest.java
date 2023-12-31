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
package org.apache.calcite.runtime;
import org.apache.calcite.linq4j.MemoryFactory;
import org.apache.calcite.test.Matchers;

import com.google.common.collect.ImmutableList;

import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/** Unit tests for {@link Automaton}. */
class AutomatonTest {

  /** Creates a Matcher that matches a list of
   * {@link org.apache.calcite.runtime.Matcher.PartialMatch} if they
   * a formatted to a given string. */
  private static <E> org.hamcrest.Matcher<List<Matcher.PartialMatch<E>>>
      isMatchList(final String value) {
    return Matchers.compose(Is.is(value),
        match -> match.stream().map(pm -> pm.rows).collect(Collectors.toList())
            .toString());
  }

  @Test void testSimple() {
    // pattern(a)
    final Pattern p = Pattern.builder().symbol("a").build();
    assertThat(p, hasToString("a"));

    final String[] rows = {"", "a", "", "a"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", s -> s.get().contains("a"))
            .build();
    final String expected = "[[a], [a]]";

    assertThat(matcher.match(rows), isMatchList(expected));
  }

  @Test void testSequence() {
    // pattern(a b)
    final Pattern p =
        Pattern.builder().symbol("a").symbol("b").seq().build();
    assertThat(p, hasToString("a b"));

    final String[] rows = {"", "a", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", s -> s.get().contains("a"))
            .add("b", s -> s.get().contains("b"))
            .build();
    final String expected = "[[a, ab], [ab, b]]";
    assertThat(matcher.match(rows), isMatchList(expected));
  }

  @Test void testStar() {
    // pattern(a* b)
    final Pattern p = Pattern.builder()
        .symbol("a").star()
        .symbol("b").seq().build();
    assertThat(p, hasToString("(a)* b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", s -> s.get().contains("a"))
            .add("b", s -> s.get().contains("b"))
            .build();
    final String expected = "[[b], [ab], [ab], [ab, a, ab], [a, ab], [b], [ab, b], [ab, a, ab, b], "
        + "[a, ab, b], [b]]";
    assertThat(matcher.match(rows), isMatchList(expected));
  }

  @Test void testPlus() {
    // pattern(a+ b)
    final Pattern p = Pattern.builder()
        .symbol("a").plus()
        .symbol("b").seq().build();
    assertThat(p, hasToString("(a)+ b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", s -> s.get().contains("a"))
            .add("b", s -> s.get().contains("b"))
            .build();
    final String expected = "[[ab, a, ab], [a, ab], [ab, b], [ab, a, ab, b], [a, ab, b]]";
    assertThat(matcher.match(rows), isMatchList(expected));
  }

  @Test void testOr() {
    // pattern(a+ b)
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").or()
        .build();
    assertThat(p, hasToString("a|b"));

    final String[] rows = {"", "a", "", "b", "", "ab", "a", "ab", "b", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("a", s -> s.get().contains("a"))
            .add("b", s -> s.get().contains("b"))
            .build();
    final String expected = "[[a], [b], [ab], [ab], [a], [ab], [ab], [b], [b]]";
    assertThat(matcher.match(rows), isMatchList(expected));
  }

  @Test void testOptional() {
    // pattern(a+ b)
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").optional().seq()
        .symbol("c").seq()
        .build();
    assertThat(p, hasToString("a b? c"));

    final String rows = "acabcabbc";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p.toAutomaton())
            .add("a", s -> s.get() == 'a')
            .add("b", s -> s.get() == 'b')
            .add("c", s -> s.get() == 'c')
            .build();
    final String expected = "[[a, c], [a, b, c]]";
    assertThat(matcher.match(chars(rows)), isMatchList(expected));
  }

  @Test void testRepeat() {
    // pattern(a b{0, 2} c)
    checkRepeat(0, 2, "a (b){0, 2} c", "[[a, c], [a, b, c], [a, b, b, c]]");
    // pattern(a b{0, 1} c)
    checkRepeat(0, 1, "a (b){0, 1} c", "[[a, c], [a, b, c]]");
    // pattern(a b{1, 1} c)
    checkRepeat(1, 1, "a (b){1} c", "[[a, b, c]]");
    // pattern(a b{1,3} c)
    checkRepeat(1, 3, "a (b){1, 3} c",
        "[[a, b, c], [a, b, b, c], [a, b, b, b, c]]");
    // pattern(a b{1,2} c)
    checkRepeat(1, 2, "a (b){1, 2} c", "[[a, b, c], [a, b, b, c]]");
    // pattern(a b{2,3} c)
    checkRepeat(2, 3, "a (b){2, 3} c", "[[a, b, b, c], [a, b, b, b, c]]");
  }

  private void checkRepeat(int minRepeat, int maxRepeat, String pattern,
      String expected) {
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").repeat(minRepeat, maxRepeat).seq()
        .symbol("c").seq()
        .build();
    assertThat(p, hasToString(pattern));

    final String rows = "acabcabbcabbbcabbbbcabdbc";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p.toAutomaton())
            .add("a", s -> s.get() == 'a')
            .add("b", s -> s.get() == 'b')
            .add("c", s -> s.get() == 'c')
            .build();
    assertThat(matcher.match(chars(rows)), isMatchList(expected));
  }

  @Test void testRepeatComposite() {
    // pattern(a (b a){1, 2} c)
    final Pattern p = Pattern.builder()
        .symbol("a")
        .symbol("b").symbol("a").seq()
        .repeat(1, 2).seq()
        .symbol("c").seq()
        .build();
    assertThat(p, hasToString("a (b a){1, 2} c"));

    final String rows = "acabcabbcabbbcabbbbcabdbcabacababcababac";
    final Matcher<Character> matcher =
        Matcher.<Character>builder(p.toAutomaton())
            .add("a", s -> s.get() == 'a')
            .add("b", s -> s.get() == 'b')
            .add("c", s -> s.get() == 'c')
            .build();
    assertThat(matcher.match(chars(rows)),
        isMatchList("[[a, b, a, c], [a, b, a, c], [a, b, a, b, a, c]]"));
  }

  @Test void testResultWithLabels() {
    // pattern(a)
    final Pattern p = Pattern.builder()
        .symbol("A")
        .symbol("B").seq()
        .build();
    assertThat(p, hasToString("A B"));

    final String[] rows = {"", "a", "ab", "a", "b"};
    final Matcher<String> matcher =
        Matcher.<String>builder(p.toAutomaton())
            .add("A", s -> s.get().contains("a"))
            .add("B", s -> s.get().contains("b"))
            .build();
    final Matcher.PartitionState<String> partitionState =
        matcher.createPartitionState(0, 0);
    final ImmutableList.Builder<Matcher.PartialMatch<String>> builder =
        ImmutableList.builder();
    MemoryFactory<String> memoryFactory = new MemoryFactory<>(0, 0);
    for (String row : rows) {
      memoryFactory.add(row);
      builder.addAll(
          matcher.matchOneWithSymbols(memoryFactory.create(), partitionState));
    }
    assertThat(builder.build(),
        hasToString("[[(A, a), (B, ab)], [(A, a), (B, b)]]"));
  }

  /** Converts a string into an iterable collection of its characters. */
  private static Iterable<Character> chars(String s) {
    return new AbstractList<Character>() {
      @Override public Character get(int index) {
        return s.charAt(index);
      }

      @Override public int size() {
        return s.length();
      }
    };
  }
}
