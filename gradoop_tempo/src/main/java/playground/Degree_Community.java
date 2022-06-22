package playground;

import data.PersonKnows;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.TemporalVertexDegree;
import org.gradoop.temporal.model.impl.operators.metric.functions.BuildTemporalDegreeTree;
import org.gradoop.temporal.model.impl.operators.metric.functions.CalculateDegreesDefaultTimesFlatMap;
import org.gradoop.temporal.model.impl.operators.metric.functions.FlatMapVertexIdEdgeInterval;

public class Degree_Community {

    public void run() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TemporalGraph communityGraph = PersonKnows.getCommunity(GradoopFlinkConfig.createConfig(env));

        /*
        communityGraph.getEdges()
                .flatMap(new FlatMapVertexIdEdgeInterval(TimeDimension.VALID_TIME,VertexDegree.IN))
                .groupBy(0)
                .reduceGroup(new BuildTemporalDegreeTree())
                .flatMap(new CalculateDegreesDefaultTimesFlatMap())
                .print();
        */

        // DataSink sink = new DOTDataSink("src/main/resources/output/o.dot", true);
        // sink.write(communityGraph.toLogicalGraph());

        // set the time dimension as VALID or TRANSACTION
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
