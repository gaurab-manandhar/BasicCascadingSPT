package cascading_assignment.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class GenderFilter extends BaseOperation implements Filter {
    private String genderValue;

    //Constructor that take values from arguments and set in the super
    public GenderFilter(String genderValue) {
        this.genderValue = genderValue;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        return !tupleEntry.getString("gender").equals(genderValue);
    }
}
