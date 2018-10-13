package cascading_assignment.assignment.question1_wordcount;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading_assignment.bufer.Word_Count_Buffer;
import cascading_assignment.function.SplitterFunction;

public class Word_Count_Main {
    /**
     * Question 1:---Do the word count of a random texts or lyrics.
     * */
    public static void main(String[] args) {

        /**
         * Resource Paths
         * */
        String source_Path = "src/main/resources/cascading_assignment/question1_wordcount/cascading_word_count_input.txt";
        String sink_Path = "src/main/resources/cascading_assignment/question1_wordcount/cascading_word_count_expected.txt";

        Tap sourceTap = new FileTap(new TextLine(new Fields("data")), source_Path);
        Tap sinkTap = new FileTap(new TextDelimited(new Fields("data", "counts"), ";"), sink_Path, SinkMode.REPLACE);

        Pipe finalPipe = new Pipe("finalPipe"); // Creates Pipe called finalPipe
        finalPipe = new Each(finalPipe, new Insert(new Fields("counts"), "1"), Fields.ALL); // Insert default count as 1
        finalPipe = new Each(finalPipe, new Fields("data"), new SplitterFunction(), Fields.REPLACE);
        finalPipe = new GroupBy(finalPipe, new Fields("data")); // Group the similar field by using the splitter function

        finalPipe = new Every(finalPipe, new Fields("counts"), new Word_Count_Buffer(), Fields.REPLACE);

        // Finally adding all the filtered data to sink pipe as finalPipe
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(finalPipe, sourceTap)
                .addTailSink(finalPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
    }

