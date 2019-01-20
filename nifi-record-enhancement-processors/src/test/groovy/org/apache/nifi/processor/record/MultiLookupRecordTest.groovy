package org.apache.nifi.processor.record

import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.processor.record.components.MockMultiKeyLookupService
import org.apache.nifi.processor.record.components.MockNoKeyLookupService
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
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
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

    @Test
    void testNoKeyLookupService() {
        def service = new MockNoKeyLookupService()
        runner.addControllerService("nokey", service)
        runner.setProperty("test.lookup_service", "nokey")
        runner.setProperty("test.record_path", "/key")
        runner.enableControllerService(service)
        runner.assertValid()
    }

    @Test
    void testMultiKeyMissing() {
        def service = new MockMultiKeyLookupService()
        runner.addControllerService("multi", service)
        runner.setProperty("test.lookup_service", "multi")
        runner.setProperty("test.record_path", "/key")
        runner.enableControllerService(service)
        runner.assertNotValid()

        def context = (MockProcessContext)runner.processContext
        def results = context.validate()

        assertEquals("Wrong size", 3, results.size())
        assertEquals(1, results.findAll { it.explanation.contains("first")}.size())
        assertEquals(1, results.findAll { it.explanation.contains("middle")}.size())
        assertEquals(1, results.findAll { it.explanation.contains("last")}.size())
    }

    @Test
    void testMissingRecordPath() {
        def service = new MockNoKeyLookupService()
        runner.addControllerService("nokey", service)
        runner.setProperty("test.lookup_service", "nokey")
        runner.enableControllerService(service)
        runner.assertNotValid()

        def results = ((MockProcessContext)runner.processContext).validate()
        assertEquals("Wrong size", 1, results.size())
        assertEquals(1, results.findAll { it.explanation.contains("path") }.size())
    }

    @Test
    void testBothServices() {
        def noKey = new MockNoKeyLookupService()
        def multi = new MockMultiKeyLookupService()
        runner.addControllerService("nokey", noKey)
        runner.addControllerService("multi", multi)
        runner.setProperty("simple.lookup_service", "nokey")
        runner.setProperty("simple.record_path", "/message")
        runner.setProperty("complex.lookup_service", "multi")
        runner.setProperty("complex.record_path", "/full_name")
        runner.setProperty("complex.first", "/first_name")
        runner.setProperty("complex.middle", "/middle_name")
        runner.setProperty("complex.last", "/last_name")
        runner.enableControllerService(noKey)
        runner.enableControllerService(multi)
        runner.assertValid()

        def schemaText = this.getClass().getResourceAsStream("/message.avsc").text
        def schema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schemaText))
        registry.addSchema("message", schema)
        schema.fields.each { field -> reader.addSchemaField(field) }

        reader.addRecord("John", "Q.", "Public", null, null)
        runner.enqueue("", [ "schema.name": "message" ])
        runner.run()

        runner.assertTransferCount(MultiLookupRecord.REL_FAILURE, 0)
        runner.assertTransferCount(MultiLookupRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(MultiLookupRecord.REL_NOT_ENRICHED, 1)
        runner.assertTransferCount(MultiLookupRecord.REL_ENRICHED, 1)

        def ff = runner.getFlowFilesForRelationship(MultiLookupRecord.REL_ENRICHED)[0]
        def raw = runner.getContentAsByteArray(ff)
        def str = new String(raw)
        def json = new JsonSlurper().parseText(str)

        assert json instanceof List
        json = json[0]

        assertEquals("John", json["first_name"])
        assertEquals("Q.", json["middle_name"])
        assertEquals("Public", json["last_name"])
        assertEquals("John Q. Public", json["full_name"])
        assertEquals("Hello, world", json["message"])
    }
}
