
import playground.Degree_CSV;
import playground.Degree_Community;

public class Main {
    public static void main(String[] args) throws Exception {
        /*
        Degree_Community personTest = new Degree_Community();
        personTest.run();
        */

        String csvPath = "/Users/lxx/Desktop/Data_temporal/citibike_1_temporal";
        Degree_CSV degree_csv = new Degree_CSV();
        degree_csv.run(csvPath);
    }
}
