import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.common.TemporalCitiBikeGraph;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.TemporalVertexDegree;

public class Bike_test {

    public void run (int numberOfParallelism) throws Exception {
        // create flink execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // set the number of parallelism
        env.setParallelism(numberOfParallelism);

        // get data
        TemporalGraph graph = TemporalCitiBikeGraph.getTemporalGraph(GradoopFlinkConfig.createConfig(env));

        // take the needed subgraph
        TemporalGraph myGraph = graph.subgraph(
                v -> v.getLabel().equals("Station"),
                e -> true
        ).verify();

        myGraph.print();

        // set the time dimension
        TimeDimension timeDimension = TimeDimension.VALID_TIME;

        // set the DegreeType
        VertexDegree degreeType = VertexDegree.BOTH;

        // set the parameter of operator
        TemporalVertexDegree operator = new TemporalVertexDegree(degreeType, timeDimension);
        operator.setIncludeVertexTime(false);

        // get the result via operator
        DataSet<Tuple4<GradoopId, Long, Long, Integer>> resultDataSet = graph.callForValue(operator);

        // print the result
        resultDataSet.print();

        // calculate the runtime
        long runtime = env.getLastJobExecutionResult().getNetRuntime();
        System.out.println("Number of Parallelism = " + numberOfParallelism + ", Runtime: " + runtime + "ms\n");
    }
}
