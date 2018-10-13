package cascading_assignment.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class SplitterFunction extends BaseOperation implements Function {

    /**
     * Constructor for getting the values
     * */
    public SplitterFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments(); // Takes all the data when the function is called.
        TupleEntry tuple = new TupleEntry(tupleEntry);

        // Putting the all tuple data in the array
        String[] row = tuple.getString("data").split("\\s+");
        for (String word : row) {
            word = word.replaceAll("[^a-zA-Z0-9]", ""); //replace all  special characters
            tuple.setString("data", word);
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
