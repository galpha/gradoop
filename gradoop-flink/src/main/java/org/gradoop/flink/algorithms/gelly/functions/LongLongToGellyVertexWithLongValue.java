/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Map function to create gelly vertices with long key and long value.
 */
public class LongLongToGellyVertexWithLongValue
  implements ElementToGellyVertex<Tuple2<Long, GradoopId>, Long, Long> {

  /**
   * Reuse object.
   */
  private Vertex<Long, Long> vertex;

  /**
   * Constructor
   */
  public LongLongToGellyVertexWithLongValue() {
    this.vertex = new Vertex<>();
  }

  /**
   * Map function to create gelly vertices with long key and long value.
   *
   * @param tuple given unique vertex id
   * @return gelly vertex
   * @throws Exception in case of failure
   *
   * {@inheritDoc}
   */
  @Override
  public Vertex<Long, Long> map(Tuple2<Long, GradoopId> tuple) throws Exception {
    vertex.setId(tuple.f0);
    vertex.setValue(tuple.f0);
    return vertex;
  }
}
