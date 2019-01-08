package org.apache.nifi.controller;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.LoggingXmlParserErrorHandler;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;

public abstract class AbstractFlowSynchronizer implements FlowSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractFlowSynchronizer.class);

    public static final URL FLOW_XSD_RESOURCE = AbstractFlowSynchronizer.class.getResource("/FlowConfiguration.xsd");
    protected final StringEncryptor encryptor;
    protected final boolean autoResumeState;
    protected final NiFiProperties nifiProperties;
    protected final ExtensionManager extensionManager;

    public AbstractFlowSynchronizer(final StringEncryptor encryptor, final NiFiProperties nifiProperties, final ExtensionManager extensionManager) {
        this.encryptor = encryptor;
        this.autoResumeState = nifiProperties.getAutoResumeState();
        this.nifiProperties = nifiProperties;
        this.extensionManager = extensionManager;
    }


    protected void checkBundleCompatibility(final Document configuration) {
        final NodeList bundleNodes = configuration.getElementsByTagName("bundle");
        for (int i = 0; i < bundleNodes.getLength(); i++) {
            final Node bundleNode = bundleNodes.item(i);
            if (bundleNode instanceof Element) {
                final Element bundleElement = (Element) bundleNode;

                final Node componentNode = bundleElement.getParentNode();
                if (componentNode instanceof Element) {
                    final Element componentElement = (Element) componentNode;
                    if (!withinTemplate(componentElement)) {
                        final String componentType = DomUtils.getChildText(componentElement, "class");
                        try {
                            BundleUtils.getBundle(extensionManager, componentType, FlowFromDOMFactory.getBundle(bundleElement));
                        } catch (IllegalStateException e) {
                            throw new MissingBundleException(e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    protected boolean withinTemplate(final Element element) {
        if ("template".equals(element.getTagName())) {
            return true;
        } else {
            final Node parentNode = element.getParentNode();
            if (parentNode instanceof Element) {
                return withinTemplate((Element) parentNode);
            } else {
                return false;
            }
        }
    }

    public static boolean isEmpty(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        final Document document = parseFlowBytes(dataFlow.getFlow());
        final Element rootElement = document.getDocumentElement();

        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootGroupElement);
        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, null, encodingVersion);

        final NodeList reportingTasks = rootElement.getElementsByTagName("reportingTask");
        final ReportingTaskDTO reportingTaskDTO = reportingTasks.getLength() == 0 ? null : FlowFromDOMFactory.getReportingTask((Element)reportingTasks.item(0),null);

        final NodeList controllerServices = rootElement.getElementsByTagName("controllerService");
        final ControllerServiceDTO controllerServiceDTO = controllerServices.getLength() == 0 ? null : FlowFromDOMFactory.getControllerService((Element)controllerServices.item(0),null);

        return isEmpty(rootGroupDto) && isEmpty(reportingTaskDTO) && isEmpty(controllerServiceDTO);
    }

    protected static Document parseFlowBytes(final byte[] flow) throws FlowSerializationException {
        // create document by parsing proposed flow bytes
        try {
            // create validating document builder
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(FLOW_XSD_RESOURCE);
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(schema);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new LoggingXmlParserErrorHandler("Flow Configuration", logger));

            // parse flow
            return (flow == null || flow.length == 0) ? null : docBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            throw new FlowSerializationException(ex);
        }
    }

    protected byte[] readFlowFromDisk() throws IOException {
        final Path flowPath = nifiProperties.getFlowConfigurationFile().toPath();
        if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
            return new byte[0];
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
             final InputStream gzipIn = new GZIPInputStream(in)) {
            FileUtils.copy(gzipIn, baos);
        }

        return baos.toByteArray();
    }

    protected static boolean isEmpty(final ProcessGroupDTO dto) {
        if (dto == null) {
            return true;
        }

        final FlowSnippetDTO contents = dto.getContents();
        if (contents == null) {
            return true;
        }

        return CollectionUtils.isEmpty(contents.getProcessors())
                && CollectionUtils.isEmpty(contents.getConnections())
                && CollectionUtils.isEmpty(contents.getFunnels())
                && CollectionUtils.isEmpty(contents.getLabels())
                && CollectionUtils.isEmpty(contents.getOutputPorts())
                && CollectionUtils.isEmpty(contents.getProcessGroups())
                && CollectionUtils.isEmpty(contents.getProcessors())
                && CollectionUtils.isEmpty(contents.getRemoteProcessGroups());
    }

    protected static boolean isEmpty(final ReportingTaskDTO reportingTaskDTO){
        return reportingTaskDTO == null || StringUtils.isEmpty(reportingTaskDTO.getName()) ;
    }

    protected static boolean isEmpty(final ControllerServiceDTO controllerServiceDTO){
        return controllerServiceDTO == null || StringUtils.isEmpty(controllerServiceDTO.getName());
    }

}
