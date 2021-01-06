package com.pjcode.processor;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

public interface Processor {

    public Record processRecord(Record record);

    public RecordSet processRecordSet(RecordSet recordSet);
}
