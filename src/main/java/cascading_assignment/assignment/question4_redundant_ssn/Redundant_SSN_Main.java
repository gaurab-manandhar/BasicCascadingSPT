package cascading_assignment.assignment.question4_redundant_ssn;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading_assignment.bufer.Redundant_Buffer;

public class Redundant_SSN_Main {
    /**
     * Question4: ---From multiple records having same SSN, take only one tuple with longest name.
     * Filter out the records if ssn is not 9 digits. Source Fields: ssn, name
     */
    public static void main(String[] args) {

        /**
         * Resource Path
         * */
        String sourcePath = "src/main/resources/cascading_assignment/question4_redundant_ssn/cascading_redundant_ssn_input.txt";
        String sinkPath = "src/main/resources/cascading_assignment/question4_redundant_ssn/cascading_redundant_ssn_expected.txt";

        /**
         * Tap for source and destination for identify the delimiter
         * */
        Tap sourceTap = new FileTap(new TextDelimited(true, "~"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

        Pipe finalPipe = new Pipe("finalPipe");
        finalPipe = new Each(finalPipe, new Fields("ssn"), new ExpressionFilter("ssn.length()!=9", String.class));
        finalPipe = new GroupBy(finalPipe, new Fields("ssn"), new Fields("name"), true);
        finalPipe = new Every(finalPipe, new Redundant_Buffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(finalPipe, sourceTap)
                .addTailSink(finalPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
