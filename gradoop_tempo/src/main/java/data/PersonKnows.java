package data;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PersonKnows {

    public static TemporalGraph getCommunity(GradoopFlinkConfig config){

        FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

        // Data
        String gdlGraph = "Community [" +
                // Vertices
                "(alice:Person {name : \"Alice\", age : 23, city : \"Leipzig\"})" +
                "(bob:Person {name : \"Bob\", age : 20, city : \"Berlin\"})" +
                "(eve:Person {name : \"Eve\", age : 24, city : \"Leipzig\"})" +
                // Edges
                "(alice)-[e1:knows {since: \"2014-1-1 22:00:00.0000\"}]->(bob)" +
                "(alice)-[e2:knows {since: \"2015-1-30 14:30:36.0000\", broken: \"2017-1-30 14:30:36.0000\"}]->(eve)" +
                "(bob)-[e3:knows {since: \"2014-3-20 06:19:59.0000\"}]->(alice)" +
                "(bob)-[e4:knows {since: \"2018-8-8 23:00:45.0000\"}]->(eve)" +
                "]";

        // load data
        loader.initDatabaseFromString(gdlGraph);

        // get LogicalGraph representation of the social network graph
        LogicalGraph networkGraph = loader.getLogicalGraph();

        // transform to temporal graph by extracting time intervals from Edges
        return TemporalGraph.fromGraph(networkGraph).transformEdges(PersonKnows::extractPeriod);
    }


    private static TemporalEdge extractPeriod(TemporalEdge current, TemporalEdge transformed) {
        transformed.setLabel(current.getLabel());
        transformed.setProperties(current.getProperties());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            String startTime = "since";
            if (current.hasProperty(startTime)) {
                transformed.setValidFrom(format.parse(current.getPropertyValue(startTime).getString()).getTime());
                transformed.removeProperty(startTime);
                String stopTime = "broken";
                if (current.hasProperty(stopTime)) {
                    transformed.setValidTo(format.parse(current.getPropertyValue(stopTime).getString()).getTime());
                    transformed.removeProperty(stopTime);
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException("Can not parse time.");
        }
        return transformed;
    }
}
