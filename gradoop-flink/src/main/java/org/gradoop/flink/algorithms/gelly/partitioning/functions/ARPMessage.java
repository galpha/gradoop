package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

public class ARPMessage extends ScatterFunction<Long, ARPVertexValue, Long, NullValue> {
  @Override
  public void sendMessages(Vertex<Long, ARPVertexValue> vertex) throws Exception {
    if ((getSuperstepNumber() % 2) == 1) {
      sendMessageTo(vertex.getId(), vertex.getValue().getCurrentPartition());
    } else {
      sendMessageToAllNeighbors(vertex.getValue().getCurrentPartition());
    }
  }
}
