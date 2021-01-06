/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pjcode.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.nifi.avro.*;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestAvroSerializationDeserialization {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testReadAvroExternalSchema() throws IOException, MalformedRecordException {
        FileInputStream avroFileInputStream = new FileInputStream(new File("src/test/resources/avro/avro_schemaless.avro"));
        Schema avroSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schemaless.avsc"));
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(avroFileInputStream, recordSchema, avroSchema);
        Record record = avroReader.nextRecord();
        assertNotNull(record);
        assertEquals(1, record.getValue("id"));
        assertNotNull(record.getValue("key"));
        assertEquals("value", record.getValue("key").toString());
    }

    @Test
    public void testReadAvroEmbeddedSchema() throws IOException, MalformedRecordException {
        FileInputStream avroFileInputStream = new FileInputStream(new File("src/test/resources/avro/avro_schema_simple_embed_schema.avro"));

        AvroRecordReader avroReader = new AvroReaderWithEmbeddedSchema(avroFileInputStream);
        Record record;
        int counter = 0;
        while ((record = avroReader.nextRecord()) != null) {
            assertNotNull(record);
            assertNotNull(record.getValue("id"));
            assertNotNull(record.getValue("username"));
            counter++;
        }
        assertEquals(3, counter);
    }

    @Test
    public void testWriteAvroRecordEmbeddedSchema() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schema_simple.avsc"));
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 123);
        values.put("username", "johnwick");
        Record record = new MapRecord(recordSchema, values);

        File file = temporaryFolder.newFile("avroSample");
        file.delete();
        file.deleteOnExit();

        FileOutputStream out = new FileOutputStream(file);
        RecordWriter avroWriter = new WriteAvroResultWithSchema(AvroTypeUtil.extractAvroSchema(recordSchema), out, CodecFactory.nullCodec());

        avroWriter.write(record);
        avroWriter.close();

        assertTrue(file.exists());
        assertTrue(file.length() > 0);

    }

    @Test
    public void testWriteAvroRecordSetEmbeddedSchema() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schema_simple.avsc"));
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        List<Record> recordList = new ArrayList<Record>();
        {
            final Map<String, Object> values = new HashMap<>();
            values.put("id", 123);
            values.put("username", "johnwick");
            recordList.add(new MapRecord(recordSchema, values));
        }

        {
            final Map<String, Object> values = new HashMap<>();
            values.put("id", 700);
            values.put("username", "jamesbond");
            recordList.add(new MapRecord(recordSchema, values));
        }

        {
            final Map<String, Object> values = new HashMap<>();
            values.put("id", 321);
            values.put("username", "johnbondy");
            recordList.add(new MapRecord(recordSchema, values));
        }

        RecordSet recordSet = new ListRecordSet(recordSchema, recordList);

        File file = temporaryFolder.newFile("avroSample");
        file.delete();
        file.deleteOnExit();

        FileOutputStream out = new FileOutputStream(file);
        RecordSetWriter avroWriter = new WriteAvroResultWithSchema(AvroTypeUtil.extractAvroSchema(recordSchema), out, CodecFactory.nullCodec());

        avroWriter.write(recordSet);
        avroWriter.close();

        assertTrue(file.exists());
        assertTrue(file.length() > 0);

    }


}