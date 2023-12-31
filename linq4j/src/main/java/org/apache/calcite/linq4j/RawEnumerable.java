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

import org.checkerframework.framework.qual.Covariant;

/**
 * Exposes the enumerator, which supports a simple iteration over a collection,
 * without the extension methods.
 *
 * <p>Just the bare methods, to make it easier to implement. Code that requires
 * the extension methods can use the static methods in {@link Extensions}.
 *
 * <p>Analogous to LINQ's System.Collections.IEnumerable (both generic
 * and non-generic variants), without the extension methods.
 *
 * @param <T> Element type
 * @see Enumerable
 */
@Covariant(0)
public interface RawEnumerable<T> {
  /**
   * Returns an enumerator that iterates through a collection.
   */
  Enumerator<T> enumerator();
}
