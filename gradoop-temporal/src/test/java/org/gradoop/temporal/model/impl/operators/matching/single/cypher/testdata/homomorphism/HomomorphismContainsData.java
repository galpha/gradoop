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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismContainsData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
    //1.[(Broadway & E14) -> (S 5 Pl & S 5 St) <- (Henry St & Grand St)]
    //2.[(Broadway & W 29) -[e13]-> (8 Ave & W31) <-[e19] (Broadway & W29)) (
    data.add(new String[] {
      "Contains_HOM_1_default_citibike",
      "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE e1!=e2 AND e1.val.contains(e2.val)",
      "expected1,expected2",
      "expected1[(s8)-[e6]->(s9)<-[e11]-(s18)], expected2[(s21)-[e13]->(s11)<-[e19]-(s21)]"
    });

    //1.[(Broadway & E14)->(S5 Pl & S 5 St)]
    //2.[(W37 St & 5 Ave)->(Hicks St & Montague St)]
    data.add(new String[] {
      "Contains_HOM_2_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.val.contains(Timestamp(2013-06-01T00:35:35)) AND " +
        "NOT b.tx.contains(Timestamp(2013-07-17))",
      "expected1,expected2",
      "expected1[(s8)-[e6]->(s9)], expected2[(s7)-[e5]->(s2)]"
    });

    // 1.[(Murray St & West St) -> (Shevchenko Pl)]
    data.add(new String[] {
      "Contains_HOM_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).contains(Interval(" +
        "Timestamp(2013-05-12), Timestamp(2013-07-28)))",
      "expected1",
      "expected1[(s24)-[e15]->(s25)]"
    });

    //(empty)
    data.add(new String[] {
      "Contains_HOM_4_default_citibike",
      "MATCH (a) WHERE NOT a.tx.contains(a.tx)",
      "[]",
      "[]"
    });

    // 1.[(Hicks St)->(Hicks St)]
    // 2.[(E20 St & Park Ave)->(E20 St & Park Ave)]
    // 3.[(Broadway & E14) -> (S 5 Pl & S 5 St)]
    data.add(new String[] {
      "Contains_HOM_5_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.tx.merge(b.tx).contains(a.tx)",
      "expected1,expected2,expected3",
      "expected1[(s2)-[e2]->(s2)], expected2[(s27)-[e17]->(s27)], " +
        "expected3[(s8)-[e6]->(s9)]"
    });

    return data;
  }
}
