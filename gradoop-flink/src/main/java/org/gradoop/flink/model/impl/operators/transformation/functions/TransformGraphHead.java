/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for graph heads.
 *
 * @param <G> the type of the graph head
 */
@FunctionAnnotation.ForwardedFields("id")
public class TransformGraphHead<G extends GraphHead> extends TransformBase<G> {

  /**
   * Factory to init modified graph head.
   */
  private final GraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  graph head modification function
   * @param epgmGraphHeadFactory      graph head factory
   */
  public TransformGraphHead(
    TransformationFunction<G> transformationFunction,
    GraphHeadFactory<G> epgmGraphHeadFactory) {
    super(transformationFunction);
    this.graphHeadFactory = checkNotNull(epgmGraphHeadFactory);
  }

  @Override
  protected G initFrom(G graphHead) {
    return graphHeadFactory.initGraphHead(graphHead.getId(), GradoopConstants.DEFAULT_GRAPH_LABEL);
  }
}
