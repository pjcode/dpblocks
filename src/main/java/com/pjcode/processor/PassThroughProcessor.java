package com.pjcode.processor;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

public class PassThroughProcessor implements Processor {

    @Override
    public Record processRecord(Record record) {
        return record;
    }

    @Override
    public RecordSet processRecordSet(RecordSet recordSet) {
        return recordSet;
    }
}
