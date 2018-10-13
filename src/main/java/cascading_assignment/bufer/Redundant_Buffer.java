package cascading_assignment.bufer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Buffer to take argument and set to tuple entry and pass by tuple
 * */

public class Redundant_Buffer extends BaseOperation implements Buffer {
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
        bufferCall.getOutputCollector().add(new TupleEntry(iterator.next()));
    }
}
