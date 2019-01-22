package org.apache.nifi.processor.record

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.csv.CSVUtils
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class CSVCleanerTest {
    TestRunner runner
    MockSchemaRegistry registry

    @Before
    void setup() {
        registry = new MockSchemaRegistry()
        runner = TestRunners.newTestRunner(CSVCleaner.class)
        runner.addControllerService("registry", registry)
        runner.setProperty(CSVCleaner.SCHEMA_REGISTRY, "registry")
        runner.enableControllerService(registry)
        runner.assertValid()
    }

    void setupSimpleSchema() {
        registry.addSchema("simple", {
            def text = CSVCleanerTest.getResourceAsStream("/csv/simple.avsc").text
            def parsed = new Schema.Parser().parse(text)
            AvroTypeUtil.createSchema(parsed)
        }())
    }

    @Test
    void test() {
        setupSimpleSchema()

        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/csv/simple.csv"), [ "schema.name": "simple" ])
        runner.run()
    }

    @Test
    void testSkipRepeatingHeaders() {
        setupSimpleSchema()

        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/csv/simple_repeat_header.csv"), [ "schema.name": "simple" ])
        runner.run()
    }
}
