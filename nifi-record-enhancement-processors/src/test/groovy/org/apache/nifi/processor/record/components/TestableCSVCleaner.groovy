package org.apache.nifi.processor.record.components

import org.apache.commons.csv.CSVFormat
import org.apache.nifi.processor.record.CSVCleaner

class TestableCSVCleaner extends CSVCleaner {
    CSVFormat getFormat() {
        this.csvFormat
    }
}
