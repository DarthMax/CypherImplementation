package benchmark;

import operators.datastructures.GraphExtended;
import org.apache.flink.api.java.ExecutionEnvironment;
import queryplan.RuleBasedOptimizer;
import queryplan.querygraph.QueryGraph;

/**
 * Created by max on 19.10.16.
 */
public abstract class Query {

  protected GraphExtended input;
  protected ExecutionEnvironment env;

  public Query(GraphExtended input, ExecutionEnvironment env) {
    this.input = input;
    this.env = env;
  }


  public Long run() throws Exception{
    QueryGraph queryGraph = getQueryGraph();

    RuleBasedOptimizer planner = new RuleBasedOptimizer(queryGraph,this.input);


    //for(VertexExtended vertex : planner.genQueryPlan().collect()) {
    //  System.out.println("vertex.getVertexId() = " + vertex.getVertexId());
    //}

    Long count = planner.genQueryPlan().count();

    return(count);
  }

  protected abstract QueryGraph getQueryGraph();

}
