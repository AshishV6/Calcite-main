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
package org.apache.calcite.linq4j;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.ObjIntConsumer;

/**
 * Pair of an element and an ordinal.
 *
 * @param <E> Element type
 */
public class Ord<E> implements Map.Entry<Integer, E> {
  public final int i;
  public final E e;

  /**
   * Creates an Ord.
   */
  public Ord(int i, E e) {
    this.i = i;
    this.e = e;
  }

  /**
   * Creates an Ord.
   */
  public static <E> Ord<E> of(int n, E e) {
    return new Ord<>(n, e);
  }

  @Override public int hashCode() {
    return Objects.hash(e, i);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof Ord
        && i == ((Ord<?>) obj).i
        && Objects.equals(e, ((Ord<?>) obj).e);
  }

  /**
   * Creates an iterable of {@code Ord}s over an iterable.
   */
  public static <E> Iterable<Ord<E>> zip(final Iterable<? extends E> iterable) {
    return () -> zip(iterable.iterator());
  }

  /**
   * Creates an iterator of {@code Ord}s over an iterator.
   */
  public static <E> Iterator<Ord<E>> zip(final Iterator<? extends E> iterator) {
    return new Iterator<Ord<E>>() {
      int n = 0;

      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public Ord<E> next() {
        return Ord.of(n++, iterator.next());
      }

      @Override public void remove() {
        iterator.remove();
      }
    };
  }

  /**
   * Returns a numbered list based on an array.
   */
  public static <E> List<Ord<E>> zip(final E[] elements) {
    return new OrdArrayList<>(elements);
  }

  /**
   * Returns a numbered list.
   */
  public static <E> List<Ord<E>> zip(final List<? extends E> elements) {
    return elements instanceof RandomAccess
        ? new OrdRandomAccessList<>(elements)
        : new OrdList<>(elements);
  }

  /**
   * Iterates over an array in reverse order.
   *
   * <p>Given the array ["a", "b", "c"], returns (2, "c") then (1, "b") then
   * (0, "a").
   */
  @SafeVarargs // heap pollution is not possible because we only read
  public static <E> Iterable<Ord<E>> reverse(E... elements) {
    return reverse(ImmutableList.copyOf(elements));
  }

  /**
   * Iterates over a list in reverse order.
   *
   * <p>Given the list ["a", "b", "c"], returns (2, "c") then (1, "b") then
   * (0, "a").
   */
  public static <E> Iterable<Ord<E>> reverse(Iterable<? extends E> elements) {
    final ImmutableList<E> elementList = ImmutableList.copyOf(elements);
    return () -> new Iterator<Ord<E>>() {
      int i = elementList.size() - 1;

      @Override public boolean hasNext() {
        return i >= 0;
      }

      @Override public Ord<E> next() {
        return Ord.of(i, elementList.get(i--));
      }
    };
  }

  @Override public Integer getKey() {
    return i;
  }

  @Override public E getValue() {
    return e;
  }

  @Override public E setValue(E value) {
    throw new UnsupportedOperationException();
  }

  /** Applies an action to every element of an iterable, passing the zero-based
   * ordinal of the element to the action.
   *
   * @see List#forEach(java.util.function.Consumer)
   * @see Map#forEach(java.util.function.BiConsumer)
   *
   * @param iterable Iterable
   * @param action The action to be performed for each element
   * @param <T> Element type
   */
  public static <T> void forEach(Iterable<T> iterable,
      ObjIntConsumer<? super T> action) {
    int i = 0;
    for (T t : iterable) {
      action.accept(t, i++);
    }
  }

  /** Applies an action to every element of an array, passing the zero-based
   * ordinal of the element to the action.
   *
   * @see List#forEach(java.util.function.Consumer)
   * @see Map#forEach(java.util.function.BiConsumer)
   *
   * @param ts Array
   * @param action The action to be performed for each element
   * @param <T> Element type
   */
  public static <T> void forEach(T[] ts,
      ObjIntConsumer<? super T> action) {
    for (int i = 0; i < ts.length; i++) {
      action.accept(ts[i], i);
    }
  }

  /** List of {@link Ord} backed by a list of elements.
   *
   * @param <E> element type */
  private static class OrdList<E> extends AbstractList<Ord<E>> {
    private final List<? extends E> elements;

    OrdList(List<? extends E> elements) {
      this.elements = elements;
    }

    @Override public Ord<E> get(int index) {
      return Ord.of(index, elements.get(index));
    }

    @Override public int size() {
      return elements.size();
    }
  }

  /** List of {@link Ord} backed by a random-access list of elements.
   *
   * @param <E> element type */
  private static class OrdRandomAccessList<E> extends OrdList<E>
      implements RandomAccess {
    OrdRandomAccessList(List<? extends E> elements) {
      super(elements);
    }
  }

  /** List of {@link Ord} backed by an array of elements.
   *
   * @param <E> element type */
  private static class OrdArrayList<E> extends AbstractList<Ord<E>>
      implements RandomAccess {
    private final E[] elements;

    OrdArrayList(E[] elements) {
      this.elements = elements;
    }

    @Override public Ord<E> get(int index) {
      return Ord.of(index, elements[index]);
    }

    @Override public int size() {
      return elements.length;
    }
  }
}
