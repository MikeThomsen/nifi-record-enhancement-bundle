package org.apache.nifi.processor.record;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CSVCleaner extends AbstractProcessor {
    private volatile String csvParser;
    private volatile CSVFormat csvFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;
    private volatile String charSet;

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("csv-cleaner-schema-registry")
        .displayName("Schema Registry")
        .description("A schema registry to use for informing cleanup decisions.")
        .identifiesControllerService(SchemaRegistry.class)
        .addValidator(Validator.VALID)
        .required(true)
        .build();
    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("csv-cleaner-schema-name")
        .displayName("Schema Name")
        .description("A hard-coded string or expression language statement for supplying the schema name to use for " +
                "checking the CSV.")
        .defaultValue("${schema.name}")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SCHEMA_REGISTRY);
        properties.add(SCHEMA_NAME);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_MODE);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.TRAILING_DELIMITER);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.CHARSET);
        return properties;
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Successfully reprocessed CSV files go here.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Failed CSV files go here.")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("If the processing is successful, the original data is sent here.")
        .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_ORIGINAL
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void storeCsvFormat(final ProcessContext context) {
        this.csvFormat = CSVUtils.createCSVFormat(context);
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        FlowFile output = session.create(input);
        try (InputStream is = session.read(input);
             OutputStream os = session.write(output)) {
            final CSVParser csvParser = new CSVParser(new InputStreamReader(new BOMInputStream(is), charSet), csvFormat);
            final CSVPrinter csvWriter = new CSVPrinter(new OutputStreamWriter(os), csvFormat);

            for (CSVRecord record : csvParser) {
                getLogger().debug("dsfasdfasdffdsfddfsdfsfds");
                getLogger().debug(record.get("First name"));
            }

            csvParser.close();
            csvWriter.close();

            is.close();
            os.close();

            session.transfer(input, REL_ORIGINAL);
            session.transfer(output, REL_SUCCESS);
        } catch (Exception ex) {
            getLogger().error("Failed to process CSV file.", ex);
            session.remove(output);
            session.transfer(input, REL_FAILURE);
        }
    }
}
