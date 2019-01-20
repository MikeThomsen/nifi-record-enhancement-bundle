package org.apache.nifi.processor.record

import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import static org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class MultiLookupRecordTest {
    TestRunner runner
    MockSchemaRegistry registry
    MockRecordParser reader

    @Before
    void setup() {
        registry = new MockSchemaRegistry()
        reader = new MockRecordParser()
        def writer = new JsonRecordSetWriter()
        runner = TestRunners.newTestRunner(MultiLookupRecord.class)
        runner.addControllerService("registry", registry)
        runner.addControllerService("reader", reader)
        runner.addControllerService("writer", writer)
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(MultiLookupRecord.READER, "reader")
        runner.setProperty(MultiLookupRecord.WRITER, "writer")
        runner.enableControllerService(registry)
        runner.enableControllerService(reader)
        runner.enableControllerService(writer)
    }

    @Test
    void testValidity() {
        runner.assertValid()
    }

    @Test
    void testNoLookupServiceInvalidity() {
        runner.setProperty("test.key", "x")
        runner.setProperty("test.record_path", "/key2")
        runner.assertNotValid()
        def context = runner.processContext
        def results = ((MockProcessContext)context).validate()
        assertEquals("Wrong size", 1, results.size())
        results.each {
            assert it.subject == "test"
            assert it.explanation.toLowerCase().contains("lookup")
        }
    }
}
