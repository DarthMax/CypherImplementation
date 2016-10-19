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


public class Query6 extends Query {

  public Query6(
    GraphExtended<Long,HashSet<String>,HashMap<String,String>,Long,String,
      HashMap<String,String>> input,
    ExecutionEnvironment env) {
    super(input,env);
  }

  protected QueryGraph getQueryGraph() {
    QueryVertex a = new QueryVertex("",new HashMap<>(),true);
    QueryVertex b = new QueryVertex("",new HashMap<>(),false);
    QueryVertex c = new QueryVertex("",new HashMap<>(),false);
    QueryVertex d = new QueryVertex("",new HashMap<>(),false);

    QueryEdge e2 = new QueryEdge(a,b,"",new HashMap<>());
    QueryEdge e1 = new QueryEdge(b,a,"",new HashMap<>());
    QueryEdge e3 = new QueryEdge(c,a,"",new HashMap<>());
    QueryEdge e4 = new QueryEdge(d,a,"",new HashMap<>());

    QueryVertex[] queryVertices = {a,b,c,d};
    QueryEdge[] queryEdges = {e1,e2,e3,e4};


    return(new QueryGraph(queryVertices, queryEdges));
  }
}