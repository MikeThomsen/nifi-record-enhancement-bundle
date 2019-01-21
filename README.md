## MultiLookupRecord Processor

### Overview

The purpose of this processor is to enable multiple lookup services to be used in the same hop in the NiFi graph. Where the normal `LookupRecord` processor is part router, part enrichment processor, this is a dedicated enrichment processor that focuses exclusively on allowing business logic to be applied to multiple fields within a single processor using lookup services.

### Use

Configuration of the processor is built around "operations." An operation is a set of dynamic properties that have this form:

```$xslt
<operation_name>.<key>
```

Consider this lookup service, which has three required keys and returns a string.

```groovy
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
```

And for this schema:

```json
{
    "name": "MessageRecord",
    "type": "record",
    "fields": [
        { "name": "first_name", "type": "string" },
        { "name": "middle_name", "type": "string" },
        { "name": "last_name", "type": "string" },
        { "name": "full_name", "type": [ "null", "string" ] },
        { "name": "message", "type": ["null", "string" ] }
    ]
}
```

It would be configured as follows:

* `my_operation.first` => `/first_name`
* `my_operation.middle` => `/middle_name`
* `my_operation.last` => `/last_name`
* `my_operation.lookup_service` => (This will create a property for selecting a service)
* `my_operation.must_pass` => `true|false`
* `my_operation.record_path` => `/full_name`

This processor is intended to be used in conjunction with implementations of the `LookupService` interface, particularly the `ScriptedLookupService` service.

## License

ASLv2
