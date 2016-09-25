package gmark;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
/*
*
* */
@SuppressWarnings("serial")
public class GMarkToGraphDataModel {
	
	private String dir;
	
	private ExecutionEnvironment env;
	
	public GMarkToGraphDataModel(String dir, ExecutionEnvironment env) {	
		this.dir = dir;
		this.env = env;
	}
	
	
	public void getGraph() throws Exception {
		DataSet<Tuple3<Long, Long, Long>> rawData = env.readCsvFile(dir + "uniprot-graph.csv")
				.fieldDelimiter(" ")
				.types(Long.class, Long.class, Long.class);
		
		
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = rawData
				.flatMap(new VerticesTransformation())
				.distinct(0);
		
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> maxVertexId = vertices.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = maxVertexId
				.cross(rawData)
				.with(new EdgesTransformation());
		
		edges.writeAsCsv(dir + "edges.csv", "\n", "|");
		vertices.writeAsCsv(dir + "vertices.csv", "\n", "|");
		env.setParallelism(1);
		env.execute();
	}
	
	private static class VerticesTransformation implements FlatMapFunction<Tuple3<Long, Long, Long>, 
		VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

		@Override
		public void flatMap(
				Tuple3<Long, Long, Long> raw,
				Collector<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertex)
				throws Exception {
			HashSet<String> label0 = new HashSet<>();
			label0.add("Protein");
			HashSet<String> label1 = new HashSet<>();
			label1.add("Gene");
			HashSet<String> label2 = new HashSet<>();
			label2.add("Organism");
			HashSet<String> label3 = new HashSet<>();
			label3.add("Article");
			HashSet<String> label4 = new HashSet<>();
			label4.add("Keyword");
			HashSet<String> label5 = new HashSet<>();
			label5.add("Author");
			HashSet<String> label6 = new HashSet<>();
			label6.add("Journal");
			HashMap<String, String> props = new HashMap<>();
			if(raw.f1 == 0) {
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label0, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label0, props));
			}
			else if(raw.f1 == 1) {
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label0, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label1, props));
			}
			else if(raw.f1 == 2) {
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label0, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label2, props));				
			}
			else if(raw.f1 == 3){
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label0, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label3, props));				
			}
			else if(raw.f1 == 4){
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label0, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label4, props));					
			}
			else if(raw.f1 == 5){
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label3, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label5, props));					
			}
			else {
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f0, label3, props));
				vertex.collect(new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(raw.f2, label6, props));					
			}
		}	
	}
	
	
	private static class EdgesTransformation implements CrossFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>, 
		Tuple3<Long, Long, Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>> {

		private long newId = 1;
		
		@Override
		public EdgeExtended<Long, Long, String, HashMap<String, String>> cross(
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> maxId,
				Tuple3<Long, Long, Long> raw) throws Exception {
			
			HashMap<String, String> props = new HashMap<>();
			if(raw.f1 == 0) {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "Interacts", props);
			}
			else if(raw.f1 == 1) {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "EncodedOn", props);	
			}
			else if(raw.f1 == 2) {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "OccursIn", props);
			}
			else if(raw.f1 == 3){
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "Reference", props);
			}
			else if(raw.f1 == 4) {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "HasKeyword", props);
			}
			else if(raw.f1 == 5) {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "AuthoredBy", props);
			}
			else  {
				return new EdgeExtended<Long, Long, String, HashMap<String, String>>(maxId.f0 + newId++, raw.f0, raw.f2, "PublishedIn", props);
			}
		}
	}
}
