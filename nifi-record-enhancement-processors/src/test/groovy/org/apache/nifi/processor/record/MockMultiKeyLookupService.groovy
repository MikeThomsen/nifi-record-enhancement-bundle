package org.apache.nifi.processor.record

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.lookup.LookupFailureException
import org.apache.nifi.lookup.LookupService

class MockMultiKeyLookupService extends AbstractControllerService implements LookupService<String> {
    @Override
    Optional<String> lookup(Map<String, Object> map) throws LookupFailureException {
        return Optional.ofNullable("${map["first"]} ${map["middle"]} ${map["last"]}")
    }

    @Override
    Class<?> getValueType() {
        return String
    }

    @Override
    Set<String> getRequiredKeys() {
        return [ "first", "middle", "last" ] as Set<String>
    }
}
