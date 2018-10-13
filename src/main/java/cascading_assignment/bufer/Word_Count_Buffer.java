package cascading_assignment.bufer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class Word_Count_Buffer extends BaseOperation implements Buffer {

    /**
     * Constructor to take arguments
     * */
    public Word_Count_Buffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        TupleEntry tuple = new TupleEntry(tupleEntryIterator.next());
        tuple.setString("counts", counter(tupleEntryIterator).toString());
        bufferCall.getOutputCollector().add(tuple);
    }
    /**
     * Function to count the aggregate avaliable tuple
     * */
    private Long counter(Iterator itr) {
        Long i = 1l; //Generate first tuple while creating tuple from TupleEntry
        while (itr.hasNext()) {
            i++;
            itr.next();
        }
        return i;
    }
}