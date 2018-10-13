package cascading_assignment.assignment.question5_sum_of_redudant_data;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading_assignment.bufer.Sum_Buffer;

public class Redundant_Sum_Main {

    /**
     * Question 5:---From a record of mbr_id and paid_amount fields,
     * if there are multiple records of same mbr_id, then sum up the paid_amount fields.
     */

    public static void main(String[] args) {

        /**
         * Resource Path
         * */
        String sourcePath = "src/main/resources/cascading_assignment/question5_sum_of_redudant_data/cascading_sum_of_redundant_data_input.txt";
        String sinkPath = "src/main/resources/cascading_assignment/question5_sum_of_redudant_data/cascading_sum_of_redundant_data_expected.txt";

        /**
         * Source and Destination Tap for seting delimiter
         * */
        Tap sourceTap = new FileTap(new TextDelimited(true, "|"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);

        /**
         * Pipe using sum buffer to get all data in tuple and using every sumup all the redundant data
         * */
        Pipe sumPipe = new Pipe("sumPipe");
        Fields groupingFields = new Fields("mbr_id");
        sumPipe = new GroupBy(sumPipe, groupingFields);
        sumPipe = new Every(sumPipe, new Fields("paid_amount"), new Sum_Buffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(sumPipe, sourceTap)
                .addTailSink(sumPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

    }
}
