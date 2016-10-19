package benchmark;

import operators.datastructures.GraphExtended;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import queryplan.RuleBasedOptimizer;
import queryplan.querygraph.QueryGraph;

import java.util.ArrayList;

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

    DataSet<ArrayList<Long>> results = planner.genResults();

    Long count = results.count();

    return(count);
  }

  protected abstract QueryGraph getQueryGraph();

}
