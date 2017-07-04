/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.api;

import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;

/**
 * Data source in analytical programs.
 */
public interface DataSink {

  /**
   * Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  void write(LogicalGraph logicalGraph) throws IOException;

  /**
   * Writes a logical graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  void write(GraphCollection graphCollection) throws IOException;

  /**
   * Writes a logical graph to the data sink.
   *
   * @param graphTransactions graph transactions
   */
  void write(GraphTransactions graphTransactions) throws IOException;

  /**
   * Writes a logical graph to the data sink with overWrite option.
   *
   * @param logicalGraph logical graph
   * @param overWrite true, if existing files should be overwritten
   */
  void write(LogicalGraph logicalGraph, boolean overWrite) throws IOException;

  /**
   * Writes a logical graph to the data sink with overWrite option.
   *
   * @param graphCollection graph collection
   * @param overWrite true, if existing files should be overwritten
   */
  void write(GraphCollection graphCollection, boolean overWrite) throws IOException;

  /**
   * Writes a logical graph to the data sink with overWrite option.
   *
   * @param graphTransactions graph transactions
   * @param overWrite true, if existing files should be overwritten
   */
  void write(GraphTransactions graphTransactions, boolean overWrite) throws IOException;
}
