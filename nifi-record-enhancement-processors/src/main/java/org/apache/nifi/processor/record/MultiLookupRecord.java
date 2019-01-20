package org.apache.nifi.processor.record;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@CapabilityDescription("This lookup processor is different in intent from the single lookup processor. It is meant to " +
        "allow multiple lookup enrichments within the same session to reduce amount of hops that have to be done for a record set " +
        "when multiple rules/changes have to be applied to enrich or correct data according to business rules and needs. It allows " +
        "the user to specify which lookups must be successul in order for a record to be considered successful, and allows records to be " +
        "separated between those that passed the criteria for success and those that did not.")
@Tags({ "record", "lookup", "enrich", "enrichment" })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class MultiLookupRecord extends AbstractProcessor {
    public static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
        .name("multi-lookup-record-reader")
        .displayName("Record Reader")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .addValidator(Validator.VALID)
        .build();
    public static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
        .name("multi-lookup-record-writer")
        .displayName("Record Writer")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .addValidator(Validator.VALID)
        .build();

    public static final AllowableValue STRAT_ALL_MUST_PASS = new AllowableValue("all",
            "All Must Pass", "On any failure, the entire record set is failed.");
    public static final AllowableValue STRAT_ANY_CAN_PASS = new AllowableValue("any", "Any",
            "Failed records don't cause the record set to be routed to failure.");
    public static final PropertyDescriptor ENRICHMENT_ERROR_STRATEGY = new PropertyDescriptor.Builder()
        .name("multi-lookup-enrichment-error-strategy")
        .displayName("Enrichment Error Strategy")
        .description("The error-handling strategy to use.")
        .allowableValues(STRAT_ANY_CAN_PASS, STRAT_ALL_MUST_PASS)
        .defaultValue(STRAT_ANY_CAN_PASS.getValue())
        .required(true)
        .addValidator(Validator.VALID)
        .build();

    public static final AllowableValue TRUE = new AllowableValue("true", "True", "");
    public static final AllowableValue FALSE = new AllowableValue("false", "False", "");

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        READER, WRITER, ENRICHMENT_ERROR_STRATEGY
    ));

    public static final Relationship REL_ENRICHED = new Relationship.Builder()
        .name("enriched")
        .description("Records that met the minimum criteria for success are sent to this relationship.")
        .build();
    public static final Relationship REL_NOT_ENRICHED = new Relationship.Builder()
        .name("not enriched")
        .description("Records that did not meet the minimum criteria for successful enrichment are sent to this relationship.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If an error occurs during processing, the original input flowfile is sent to this relationship.")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Whenever there is not an error in the processing, the original input flowfile is sent to this relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_ENRICHED, REL_NOT_ENRICHED, REL_FAILURE, REL_ORIGINAL
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        String[] split = name.split("\\.");
        if (split.length != 2) {
            throw new ProcessException("Name must be in format \"<operation_name>.<property>\"");
        }

        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder().name(name).required(true);

        if (split[1].equals("lookup_service")) {
            builder.addValidator(Validator.VALID)
                .identifiesControllerService(LookupService.class);
        } else if (split[1].equals("must_pass")) {
            builder.allowableValues(TRUE, FALSE)
                .description("Controlls whether or not this operation must succeed for the result to be considered a success.")
                .defaultValue(TRUE.getValue());
        } else {
            builder.addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES);
        }

        return builder.build();
    }

    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile boolean isAllOrNothing = false;
    private volatile List<LookupService> services;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        this.isAllOrNothing = context.getProperty(ENRICHMENT_ERROR_STRATEGY).getValue().equals(STRAT_ALL_MUST_PASS.getValue());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        Map<String, Map<String, PropertyDescriptor>> operations = new HashMap<>();

        validationContext.getProperties().keySet()
            .stream().filter(prop -> prop.isDynamic())
            .forEach(prop -> {
                String name = prop.getName();
                String[] parts = name.split("\\.");
                Map<String, PropertyDescriptor> props;
                if (!operations.containsKey(parts[0])) {
                    props = new HashMap<>();
                    operations.put(parts[0], props);
                } else {
                    props = operations.get(parts[0]);
                }

                props.put(parts[1], prop);
            });

        operations
            .entrySet()
            .stream()
            .forEach(entry -> {
                Map<String, PropertyDescriptor> props = entry.getValue();
                PropertyDescriptor ls = props.get("lookup_service");
                if (ls != null) {
                    LookupService service = validationContext.getProperty(ls).asControllerService(LookupService.class);
                    Set<String> requiredKeys = service.getRequiredKeys();
                    List<String> missingKeys = new ArrayList<>();
                    requiredKeys
                        .stream()
                        .forEach(key -> {
                            if (!props.containsKey(key)) {
                                missingKeys.add(key);
                            }
                        });
                    if (missingKeys.size() > 0) {
                        missingKeys.stream().forEach(key -> {
                            results.add(new ValidationResult.Builder().subject(entry.getKey())
                                    .explanation(String.format("Configured lookup service is missing required key \"%s\"", key)).build());
                        });
                    }
                } else {
                    results.add(new ValidationResult.Builder().subject(entry.getKey()).explanation("No lookup service configured.").build());
                }
            });

        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        FlowFile enriched = session.create(input);
        FlowFile notEnriched = session.create(input);

        try (InputStream is = session.read(input);
             OutputStream eOS = session.write(enriched);
             OutputStream nOS = session.write(notEnriched))
        {
            RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());
            RecordSetWriter eWriter = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), null), eOS);
            RecordSetWriter nWriter = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), null), nOS);

            eWriter.beginRecordSet();
            nWriter.beginRecordSet();

            Record record;
            while ((record = reader.nextRecord()) != null) {
                
            }

            eWriter.finishRecordSet();
            nWriter.finishRecordSet();
            eWriter.close();
            nWriter.close();

            is.close();
            eOS.close();
            nOS.close();

            session.getProvenanceReporter().modifyContent(enriched);
            session.getProvenanceReporter().modifyContent(notEnriched);
            session.transfer(enriched, REL_ENRICHED);
            session.transfer(notEnriched, REL_NOT_ENRICHED);
            session.transfer(input, REL_ORIGINAL);
        } catch (Exception ex) {
            getLogger().error("Error handling enrichment.", ex);
            session.remove(enriched);
            session.remove(notEnriched);
            session.transfer(input, REL_FAILURE);
        }
    }
}
