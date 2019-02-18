package org.gradoop.utils.importer.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LDBCVertexToEPGMVertex implements MapFunction<LDBCVertex, ImportVertex<Long>> {

  private ImportVertex<Long> reuse;

  public LDBCVertexToEPGMVertex() {
    reuse = new ImportVertex<>();
  }

  @Override
  public ImportVertex<Long> map(LDBCVertex ldbcVertex) throws Exception {

    reuse.setId(ldbcVertex.getVertexId());

    reuse.setLabel(ldbcVertex.getLabel());

    Map<String, Object> properties = ldbcVertex.getProperties();

    Properties vertexProperties = Properties.create();

    for (Map.Entry<String, Object> props: properties.entrySet()) {


      if (props.getKey().equals("speaks") || props.getKey().equals("email")) {
        List<PropertyValue> propertyList = new ArrayList<>();
        for (String entity: (List<String>) props.getValue()) {
          propertyList.add(PropertyValue.create(entity));
        }
        vertexProperties.set(props.getKey(), propertyList);
        continue;
      }
      vertexProperties.set(props.getKey(), props.getValue());
    }

    reuse.setProperties(vertexProperties);

    return reuse;
  }
}
