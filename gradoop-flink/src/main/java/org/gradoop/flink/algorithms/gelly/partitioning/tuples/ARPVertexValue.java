package org.gradoop.flink.algorithms.gelly.partitioning.tuples;

public class ARPVertexValue {
  private long currentPartition;
  private long desiredPartition;
  private long degree;

  public ARPVertexValue(long currentPartition,
    long desiredPartition) {
    this.currentPartition = currentPartition;
    this.desiredPartition = desiredPartition;
  }

  public ARPVertexValue(long currentPartition, long desiredPartition, long degree){
    this.currentPartition = currentPartition;
    this.desiredPartition = desiredPartition;
    this.degree = degree;
  }

  @Override
  public String toString(){
    return Long.toString(this.currentPartition);
  }

  public long getCurrentPartition() {
    return currentPartition;
  }

  public void setCurrentPartition(long currentPartition) {
    this.currentPartition = currentPartition;
  }

  public long getDesiredPartition() {
    return desiredPartition;
  }

  public void setDesiredPartition(long desiredPartition) {
    this.desiredPartition = desiredPartition;
  }

  public long getDegree() {
    return degree;
  }

  public void setDegree(long degree) {
    this.degree = degree;
  }
}
