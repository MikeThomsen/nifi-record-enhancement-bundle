package org.apache.nifi.processor.record.components

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.lookup.LookupFailureException
import org.apache.nifi.lookup.LookupService

class MockNoKeyLookupService extends AbstractControllerService implements LookupService<String> {
    @Override
    Optional<String> lookup(Map<String, Object> map) throws LookupFailureException {
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
