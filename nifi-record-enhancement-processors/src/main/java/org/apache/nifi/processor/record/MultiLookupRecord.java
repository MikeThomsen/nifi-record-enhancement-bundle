package org.apache.nifi.processor.record;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        return null;
    }

    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile boolean isAllOrNothing = false;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        this.isAllOrNothing = context.getProperty(ENRICHMENT_ERROR_STRATEGY).getValue().equals(STRAT_ALL_MUST_PASS.getValue());
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
