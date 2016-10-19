package benchmark.queries;

import benchmark.Query;
import operators.datastructures.GraphExtended;
import org.apache.flink.api.java.ExecutionEnvironment;
import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by max on 18.10.16.
 */

public class Query1 extends Query{

  public Query1(
    GraphExtended<Long,HashSet<String>,HashMap<String,String>,Long,String,
      HashMap<String,String>> input,
    ExecutionEnvironment env) {
    super(input,env);
  }

  protected QueryGraph getQueryGraph() {

    QueryVertex a = new QueryVertex("",new HashMap<>(),true);
    QueryVertex b = new QueryVertex("",new HashMap<>(),false);

    QueryEdge e1 = new QueryEdge(a,b,"",new HashMap<>());
    QueryEdge e2 = new QueryEdge(b,a,"",new HashMap<>());

    QueryVertex[] queryVertices = {a,b};
    QueryEdge[] queryEdges = {e1,e2};


    return(new QueryGraph(queryVertices, queryEdges));
  }
}
