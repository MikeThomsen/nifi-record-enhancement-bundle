package org.apache.nifi.processor.record;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        READER, WRITER
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }
    }
}
