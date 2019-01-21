package org.apache.nifi.processor.record.components

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.lookup.LookupFailureException
import org.apache.nifi.lookup.LookupService

class MockNoKeyLookupService extends AbstractControllerService implements LookupService<String> {
    boolean causeFailure
    int failLimit = 1
    int failed

    @Override
    Optional<String> lookup(Map<String, Object> map) throws LookupFailureException {
        if (causeFailure && ++failed <= failLimit) {
            throw new RuntimeException("Forced failure")
        }

        return Optional.ofNullable("Hello, world")
    }

    @Override
    Class<?> getValueType() {
        return String
    }

    @Override
    Set<String> getRequiredKeys() {
        return [] as Set<String>
    }
}
