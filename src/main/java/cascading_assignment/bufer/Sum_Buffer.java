package cascading_assignment.bufer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Buffer to take argument and set to tuple entry and pass by tuple
 * */
public class Sum_Buffer extends BaseOperation implements Buffer {

    public Sum_Buffer(){
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        TupleEntry firstTuple = new TupleEntry(tupleEntryIterator.next());

        int sum = firstTuple.getInteger("paid_amount");
        while (tupleEntryIterator.hasNext()) {
            sum = sum + tupleEntryIterator.next().getInteger("paid_amount");
        }
        firstTuple.setInteger("paid_amount", sum);
        bufferCall.getOutputCollector().add(firstTuple);
    }
}
