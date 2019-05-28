package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.algorithms.gelly.partitioning.GradoopARPartitioning;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

public class ARPMessage extends ScatterFunction<Long, ARPVertexValue, Long, NullValue> {


  @Override
  public void sendMessages(Vertex<Long, ARPVertexValue> vertex) throws Exception {

//    if (getSuperstepNumber() == 1) {
//      notifyAggregator(getCapacityAggregatorString(vertex.getValue().getCurrentPartition()));
//      sendMessageToAllNeighbors(vertex.getValue().getCurrentPartition());
//    } else {
//      if ((getSuperstepNumber() % 2) == 1) {
//        notifyAggregator(getCapacityAggregatorString(vertex.getValue().getCurrentPartition()));
//        sendMessageTo(vertex.getId(), vertex.getValue().getCurrentPartition());
//      } else {
//        notifyAggregator(getCapacityAggregatorString(vertex.getValue().getCurrentPartition()));
//        sendMessageToAllNeighbors(vertex.getValue().getCurrentPartition());
//      }
//    }

    notifyAggregator(getCapacityAggregatorString(vertex.getValue().getCurrentPartition()));
    sendMessageToAllNeighbors(vertex.getValue().getCurrentPartition());

  }


  private String getCapacityAggregatorString(long desiredPartition) {
    String CAPACITY_AGGREGATOR_PREFIX = GradoopARPartitioning.CAPACITY_AGGREGATOR_PREFIX;
    return CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
  }

  /**
   * Notify Aggregator
   *
   * @param aggregatorString Aggregator name
   */
  private void notifyAggregator(final String aggregatorString) {
    LongSumAggregator aggregator = getIterationAggregator(aggregatorString);
    aggregator.aggregate(1);
  }
}
