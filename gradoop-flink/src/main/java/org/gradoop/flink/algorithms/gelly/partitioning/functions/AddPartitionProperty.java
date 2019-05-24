package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddPartitionProperty
  implements JoinFunction<Tuple3<Long, GradoopId, Long>, Vertex, Vertex> {

  private final String PARTITION_KEY;

  public AddPartitionProperty(String property_key) {
    this.PARTITION_KEY = property_key;
  }

  @Override
  public Vertex join(Tuple3<Long, GradoopId, Long> resultTuple, Vertex vertex) throws Exception {

    vertex.setProperty(PARTITION_KEY, resultTuple.f2);

    return vertex;
  }
}
