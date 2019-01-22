package org.apache.nifi.processor.record

import org.apache.nifi.csv.CSVUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class CSVCleanerTest {
    TestRunner runner

    @Before
    void setup() {
        runner = TestRunners.newTestRunner(CSVCleaner.class)
        runner.assertValid()
    }

    //@Test
    void test() {
        runner.setProperty(CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_NONE)
        runner.enqueue(this.getClass().getResourceAsStream("/simple.csv"))
        runner.run()
    }
}
