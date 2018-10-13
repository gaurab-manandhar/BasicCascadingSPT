package cascading_assignment.assignment.question2_demerge_pipe;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading_assignment.function.GenderFilter;

public class DeMerge_Pipe_Main {
    /**
     *Question 2:------Demerge a single pipe to mPipe and fPipe on the basis of gender “M” and “F”.
     */

    public static void main(String[] args) {

        /**
         * Resource Paths (source, Male , Female and Unknown)
         * */
        String sourcePath = "src/main/resources/cascading_assignment/question2_demerge_pipe/cascading_demerge_gender_input.txt";
        String sinkMPath = "src/main/resources/cascading_assignment/question2_demerge_pipe/cascading_demerge_male_expected.txt";
        String sinkFPath = "src/main/resources/cascading_assignment/question2_demerge_pipe/cascading_demerge_female_expected.txt";
        String sinkUPath = "src/main/resources/cascading_assignment/question2_demerge_pipe/cascading_demerge_unknown_group_expected.txt";

        /**
         * Tap for defining the input and output delimeter
         * */
        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkMTap = new FileTap(new TextDelimited(true, "|"), sinkMPath, SinkMode.REPLACE);
        Tap sinkFTap = new FileTap(new TextDelimited(true, "|"), sinkFPath, SinkMode.REPLACE);
        Tap sinkUTap = new FileTap(new TextDelimited(true, "|"), sinkUPath, SinkMode.REPLACE);

        /**
         * Main pipe to demerge pipe for gender seperation
         * */
        Pipe genderPipe = new Pipe("genderPipe");

        Pipe mPipe = new Pipe("malePipe", genderPipe);
        Pipe fPipe = new Pipe("femalePipe", genderPipe);
        Pipe uPipe = new Pipe("unknownPipe", genderPipe);

        mPipe = new Each(mPipe, new Fields("gender"), new GenderFilter("M"));
        fPipe = new Each(fPipe, new Fields("gender"), new GenderFilter("F"));
        uPipe = new Each(uPipe, new Fields("gender"), new GenderFilter("U"));

        /**
         * Adding filter output to the  diferrent sink pipe
         * */

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(genderPipe, sourceTap)
                .addTailSink(mPipe, sinkMTap)
                .addTailSink(fPipe, sinkFTap)
                .addTailSink(uPipe, sinkUTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
