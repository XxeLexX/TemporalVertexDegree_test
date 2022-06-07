package data;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
public class PersonKnows {
    public static TemporalGraph getCommunity(GradoopFlinkConfig config){
        FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

        // Data
        String gdlGraph = "Community [" +
                // Vertices
                "(alice:Person)" +
                "(bob:Person)" +
                "(eve:Person)" +
                // Edges
                "(alice)-[e1:knows {since: \"2014-1-1 22:00:00.0000\"}]->(bob)" +
                "(alice)-[e2:knows {since: \"2015-1-30 14:30:36.0000\"}]->(eve)" +
                "(bob)-[e3:knows {since: \"2014-3-20 06:19:59.0000\"}]->(alice)" +
                "(bob)-[e4:knows {since: \"2018-8-8 23:00:45.0000\"}]->(eve)" +
                "]";

        // load data
        loader.initDatabaseFromString(gdlGraph);

        // get LogicalGraph representation of the social network graph
        LogicalGraph networkGraph = loader.getLogicalGraph();

        // transform to temporal graph by extracting time intervals from vertices
        return TemporalGraph.fromGraph(networkGraph);
    }


}
