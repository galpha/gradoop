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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismComparisonData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
    // 2. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
    data.add(new String[] {
      "Comparison_ISO_1_default_citibike",
      "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE " +
        "e1.val_from>e2.val_from AND a.id=475",
      "expected1,expected2",
      "expected1[(s3)-[e3]->(s4) (s0)-[e0]->(s1)], " +
        "expected2[(s3)-[e3]->(s4) (s0)-[e1]->(s1)]"
    });

    // 1. [(Broadway & E14) -> (S 5 Pl) <- (Henry St & Grand St)]
    data.add(new String[] {
      "Comparison_ISO_2_default_citibike",
      "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=532 AND e1.tx_from>e2.tx_from " +
        "AND e2.tx_to>=e1.tx_to",
      "expected1",
      "expected1[(s8)-[e6]->(s9)<-[e11]-(s18)]"
    });

    // 1.[(9 Ave & W22) -> (8 Ave & W31) <-[edgeId:19]- (Broadway & W29)]
    // 2.[(9 Ave & W22) -> (8 Ave & W31) <-[edgeId:13]- (Broadway & W29)]
    data.add(new String[] {
      "Comparison_ISO_3_default_citibike",
      "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=521 AND e1.bikeID=16100 " +
        "AND e2.val_from > e1.val_from",
      "expected1,expected2",
      "expected1[(s10)-[e7]->(s11)<-[e19]-(s21)], " +
        "expected2[(s10)-[e7]->(s11)<-[e13]-(s21)]"
    });

    return data;
  }
}
