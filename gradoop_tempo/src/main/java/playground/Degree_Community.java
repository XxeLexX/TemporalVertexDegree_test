package playground;

import data.PersonKnows;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.TemporalVertexDegree;

public class Degree_Community {

    public void run() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TemporalGraph communityGraph = PersonKnows.getCommunity(GradoopFlinkConfig.createConfig(env));

        communityGraph.print();

        // set the time dimension as VALID or TRANSFORM
        TimeDimension timeDimension = TimeDimension.VALID_TIME;

        // set the DegreeType as IN, OUT or BOTH
        VertexDegree degreeType = VertexDegree.IN;

        // set the parameter of operator
        TemporalVertexDegree operator = new TemporalVertexDegree(degreeType, timeDimension);
        operator.setIncludeVertexTime(false);

        // get the result via operator
        DataSet<Tuple4<GradoopId, Long, Long, Integer>> resultDataSet = communityGraph.callForValue(operator);

        resultDataSet.print();
    }
}
