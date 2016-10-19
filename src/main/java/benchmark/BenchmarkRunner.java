package benchmark;

import benchmark.queries.*;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;


/**
 * Created by max on 18.10.16.
 */
public class BenchmarkRunner {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GraphReader graphReader = new GraphReader(args[0], env);

    Query query;

    switch (args[1]) {
    case "1":
      //(a)-[]->(b)-[]->(a)
      query = new Query1(graphReader.loadGraph(), env);
      break;
    case "2":
      //(c)<--(a)-->(b)
      query = new Query2(graphReader.loadGraph(), env);
      break;
    case "3":
      //(a)<--(b)-->(c)-->(a)
      query = new Query3(graphReader.loadGraph(), env);
      break;
    case "4":
      //(a)->(b)->(c)->(a)
        query = new Query4(graphReader.loadGraph(), env);
      break;
    case "5":
      //(a)->(b)->(a)<-(c)
      query = new Query5(graphReader.loadGraph(), env);
      break;
    case "6":
      //(a)->(b)->(a)<-(c),
      //(d)->(a)
      query = new Query6(graphReader.loadGraph(), env);
      break;
    case "7":
      //(c)<-(a)->(d),
      //(c)<-(b)->(d)
      query = new Query7(graphReader.loadGraph(), env);
      break;
    default:
      query = new Query1(graphReader.loadGraph(), env);
    }

    System.out.println("Results = " + query.run());
    System.out.println("Running Time = "+ env.getLastJobExecutionResult()
      .getNetRuntime(TimeUnit.SECONDS));
  }
}





