package org.apache.nifi.processor.record

import org.apache.avro.Schema
import org.apache.commons.csv.CSVParser
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.csv.CSVUtils
import org.apache.nifi.processor.record.components.TestableCSVCleaner
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class CSVCleanerTest {
    TestRunner runner
    MockSchemaRegistry registry
    TestableCSVCleaner processor

    @Before
    void setup() {
        processor = new TestableCSVCleaner()
        registry = new MockSchemaRegistry()
        runner = TestRunners.newTestRunner(processor)
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

    void testTransferCount(int success, int failure, int original) {
        runner.assertTransferCount(CSVCleaner.REL_FAILURE, failure)
        runner.assertTransferCount(CSVCleaner.REL_SUCCESS, success)
        runner.assertTransferCount(CSVCleaner.REL_ORIGINAL, original)
    }

    @Test
    void test() {
        setupSimpleSchema()

        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/csv/simple.csv"), [ "schema.name": "simple" ])
        runner.run()

        testTransferCount(1, 0, 1)
    }

    @Test
    void testSkipRepeatingHeaders() {
        setupSimpleSchema()

        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/csv/simple_repeat_header.csv"), [ "schema.name": "simple" ])
        runner.run()

        testTransferCount(1, 0, 1)

        def raw = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(CSVCleaner.REL_SUCCESS)[0])
        def csvReader = new CSVParser(new InputStreamReader(new ByteArrayInputStream(raw)), processor.csvFormat)
        def iterator = csvReader.iterator()
        assert iterator.hasNext()
        def header = iterator.next()
        assert header.size() == 2
        assert header[0] == "First Name"
        assert header[1] == "Last Name"

        int count = 0
        iterator.each {
            assert it.size() == 2
            count++
        }
        assert count == 6
    }

    @Test
    void testCSVFromBadExcelExample() {
        registry.addSchema("extra", {
            def text = CSVCleanerTest.getResourceAsStream("/csv/complex.avsc").text
            def parsed = new Schema.Parser().parse(text)
            AvroTypeUtil.createSchema(parsed)
        }())

        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/csv/with_bad_data.csv"), [ "schema.name": "extra" ])
        runner.run()

        testTransferCount(1, 0, 1)

        def ff = runner.getFlowFilesForRelationship(CSVCleaner.REL_SUCCESS)[0]
        def raw = runner.getContentAsByteArray(ff)
        def csvReader = new CSVParser(new InputStreamReader(new ByteArrayInputStream(raw)), processor.csvFormat)
        def iterator = csvReader.iterator()
        assert iterator.hasNext()
        def header = iterator.next()
        assert header.size() == 4
        assert header[0] == "BFirst Name"
        assert header[1] == "BLast Name"
        assert header[2] == "SFirst Name"
        assert header[3] == "SLast Name"

        int count = 0
        iterator.each {
            assert it.size() == 4
            count++
        }
        assert count == 3

        def recordCount = ff.getAttribute("record.count")
        def originalCount = ff.getAttribute("line.count.original")

        assert recordCount == "3"
        assert originalCount == "8"
    }
}
