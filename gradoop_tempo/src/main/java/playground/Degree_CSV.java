package playground;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.operators.metric.TemporalVertexDegree;
import org.gradoop.temporal.util.TemporalGradoopConfig;

public class Degree_CSV {
    public void run(String filename) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TemporalDataSource dataSource = new TemporalCSVDataSource(filename, TemporalGradoopConfig.createConfig(env));

        // set the time dimension as VALID or TRANSACTION
        TimeDimension timeDimension = TimeDimension.VALID_TIME;

        // set the DegreeType as IN, OUT or BOTH
        VertexDegree degreeType = VertexDegree.IN;

        // set the parameter of operator
        TemporalVertexDegree operator = new TemporalVertexDegree(degreeType, timeDimension);
        operator.setIncludeVertexTime(false);

        // get the result via operator
        DataSet<Tuple4<GradoopId, Long, Long, Integer>> resultDataSet = dataSource.getTemporalGraph().callForValue(operator);

        resultDataSet.print();
    }
}
