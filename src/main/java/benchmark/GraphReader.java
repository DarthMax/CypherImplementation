package benchmark;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by max on 18.10.16.
 */
public class GraphReader {

  private String filePath;
  private ExecutionEnvironment env;


  public GraphReader(String filePath, ExecutionEnvironment env) {
    this.filePath = filePath;
    this.env = env;
  }


  public GraphExtended<Long,HashSet<String>,HashMap<String,String>,Long,String,
    HashMap<String,String>> loadGraph() {
    DataSet<Tuple2<Long,Long>> edgeList = env.readCsvFile(filePath)
      .fieldDelimiter("\t")
      .types(Long.class,Long.class);


    DataSet<Long> vertexIds = edgeList.flatMap(
      new FlatMapFunction<Tuple2<Long, Long>, Long>() {
        @Override
        public void flatMap(Tuple2<Long, Long> edgeCandidate,
          Collector<Long> collector) throws Exception {
          collector.collect(edgeCandidate.f0);
          collector.collect(edgeCandidate.f1);
        }
      }
      ).distinct();

    DataSet<VertexExtended<Long,HashSet<String>,HashMap<String,String>>>
      vertices = vertexIds.map(new VertexMapFunction());


    DataSet<EdgeExtended<Long,Long,String,HashMap<String,String>>>
      edges = edgeList.map(new EdgeListToEdges());

    return(GraphExtended.fromDataSet(vertices,edges,env));
  }




  private class VertexMapFunction implements MapFunction<
    Long,
    VertexExtended<Long,HashSet<String>,HashMap<String,String>>
    > {

    @Override
    public VertexExtended<Long,HashSet<String>,HashMap<String,String>>
      map(Long vertexId) throws Exception {

        HashSet<String> labels = new HashSet<>();
        HashMap<String,String> properties = new HashMap<>();

        return(new VertexExtended<>(vertexId,labels,properties));
    }
  }


  private class EdgeListToEdges implements MapFunction<
      Tuple2<Long,Long>,
      EdgeExtended<Long,Long,String,HashMap<String,String>>
    > {

    private Random gen;

 		EdgeListToEdges() { this.gen = new Random(); }

    @Override
    public EdgeExtended<Long,Long,String,HashMap<String,String>> map
      (Tuple2<Long,Long> edge_entry)
      throws Exception {

      Long id = gen.nextLong();
      String label = "Label";
      HashMap<String,String> properties = new HashMap<>();

      return(new EdgeExtended<>(id,edge_entry.f0,edge_entry.f1,label,properties));
    }
  }
}
