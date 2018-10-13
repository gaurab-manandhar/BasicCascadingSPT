package cascading_assignment.assignment.question3_age_calculation;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading_assignment.function.Age_Calculation_Function;

public class Age_Calculation_Main {
    /**
     * Question 3 :---- Calculate and store age on the basis of provided DOB in source file.
     * */
    public static void main(String[] args) {

        /**
         *Resource Path
         * */
        String sourcePath = "src/main/resources/cascading_assignment/question3_age_calculation/cascading_age_calculation_input.txt";
        String sinkPath = "src/main/resources/cascading_assignment/question3_age_calculation/cascading_age_calculation_expected.txt";
        /**
         * Tap for source pipe and destination pipe
         * */
        Tap sourceTap = new FileTap(new TextDelimited(true, ";", "\""), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

        /**
         * Pipe with source and filter data update to sink pipe
         * */
        Pipe pipe = new Pipe("dobPipe");
        pipe = new Each(pipe, new Insert(new Fields("age"),""), Fields.ALL);
        pipe = new Each(pipe, new Fields("date_of_birth","age"), new Age_Calculation_Function(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
