import Data.TemporalCitiBikeGraph;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
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

        // set the time dimension
        TimeDimension timeDimension = TimeDimension.VALID_TIME;

        // set the DegreeType
        VertexDegree degreeType = VertexDegree.BOTH;

        // get the result via operator
        TemporalVertexDegree operator = new TemporalVertexDegree(degreeType, timeDimension);
        DataSet<Tuple4<GradoopId, Long, Long, Integer>> resultDataSet = graph.callForValue(operator);

        // print the result on the console
        resultDataSet.print();

        // calculate the runtime
        long runtime = env.getLastJobExecutionResult().getNetRuntime();
        System.out.println("Number of Parallelism = " + numberOfParallelism + ", Runtime: " + runtime + "ms\n");
    }
}
