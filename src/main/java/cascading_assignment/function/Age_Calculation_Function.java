package cascading_assignment.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.time.Year;

public class Age_Calculation_Function extends BaseOperation implements Function {

    /**
     * Constructor that receive the arguments f
     * */
    public Age_Calculation_Function() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String dob = tuple.getString("date_of_birth"); // take a tuple of date of birth
        tuple.setInteger("age", getAge(dob));
        functionCall.getOutputCollector().add(tuple);
    }

    /**
     * Function that return the current age based on comparing the current date with given date of birth
     * */
    private int getAge(String dob) {
        return Year.now().compareTo(Year.parse(dob.substring(0, 4)));
    }
}
