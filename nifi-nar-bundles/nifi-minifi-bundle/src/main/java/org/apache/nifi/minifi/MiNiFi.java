/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.documentation.DocGenerator;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarAutoLoader;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarLoader;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardNarLoader;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiWebConfigurationContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

public class MiNiFi implements NiFiServer {

    private final NiFiProperties props;

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private ExtensionMapping extensionMapping;
    private NarAutoLoader narAutoLoader;


    public MiNiFi(final NiFiProperties props, final Set<Bundle> bundles) {
        this.props = props;

//        final QueuedThreadPool threadPool = new QueuedThreadPool(props.getWebThreads());
//        threadPool.setName("NiFi Web Server");
//
//        // create the server
//        this.server = new Server(threadPool);

//
//        // enable the annotation based configuration to ensure the jsp container is initialized properly
//        final Configuration.ClassList classlist = Configuration.ClassList.setServerDefault(server);
//        classlist.addBefore(JettyWebXmlConfiguration.class.getName(), AnnotationConfiguration.class.getName());
//
    }

    @Override
    public void start() {
        try {
            // Create a standard extension manager and discover extensions
            final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
            extensionManager.discoverExtensions(systemBundle, bundles);
            extensionManager.logClassLoaderMapping();

            // Set the extension manager into the holder which makes it available to the Spring context via a factory bean
            ExtensionManagerHolder.init(extensionManager);

            // start the server
            server.start();

            // ensure everything started successfully
            for (Handler handler : server.getChildHandlers()) {
                // see if the handler is a web app
                if (handler instanceof WebAppContext) {
                    WebAppContext context = (WebAppContext) handler;

                    // see if this webapp had any exceptions that would
                    // cause it to be unavailable
                    if (context.getUnavailableException() != null) {
                        startUpFailure(context.getUnavailableException());
                    }
                }
            }

            // ensure the appropriate wars deployed successfully before injecting the NiFi context and security filters
            // this must be done after starting the server (and ensuring there were no start up failures)
            if (webApiContext != null) {
                // give the web api the component ui extensions
                final ServletContext webApiServletContext = webApiContext.getServletHandler().getServletContext();
                webApiServletContext.setAttribute("nifi-ui-extensions", componentUiExtensions);

                // get the application context
                final WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(webApiServletContext);
                final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);
                final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");

                // component ui extensions
                performInjectionForComponentUis(componentUiExtensionWebContexts, configurationContext, securityFilter);

                // content viewer extensions
                performInjectionForContentViewerUis(contentViewerWebContexts, securityFilter);

                // content viewer controller
                if (webContentViewerContext != null) {
                    final ContentAccess contentAccess = webApplicationContext.getBean("contentAccess", ContentAccess.class);

                    // add the content access
                    final ServletContext webContentViewerServletContext = webContentViewerContext.getServletHandler().getServletContext();
                    webContentViewerServletContext.setAttribute("nifi-content-access", contentAccess);

                    if (securityFilter != null) {
                        webContentViewerContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                    }
                }
            }

            // ensure the web document war was loaded and provide the extension mapping
            if (webDocsContext != null) {
                final ServletContext webDocsServletContext = webDocsContext.getServletHandler().getServletContext();
                webDocsServletContext.setAttribute("nifi-extension-mapping", extensionMapping);
            }

            // if this nifi is a node in a cluster, start the flow service and load the flow - the
            // flow service is loaded here for clustered nodes because the loading of the flow will
            // initialize the connection between the node and the NCM. if the node connects (starts
            // heartbeating, etc), the NCM may issue web requests before the application (wars) have
            // finished loading. this results in the node being disconnected since its unable to
            // successfully respond to the requests. to resolve this, flow loading was moved to here
            // (after the wars have been successfully deployed) when this nifi instance is a node
            // in a cluster
            if (props.isNode()) {

                FlowService flowService = null;
                try {

                    logger.info("Loading Flow...");

                    ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(webApiContext.getServletContext());
                    flowService = ctx.getBean("flowService", FlowService.class);

                    // start and load the flow
                    flowService.start();
                    flowService.load(null);

                    logger.info("Flow loaded successfully.");

                } catch (BeansException | LifeCycleStartException | IOException | FlowSerializationException | FlowSynchronizationException | UninheritableFlowException e) {
                    // ensure the flow service is terminated
                    if (flowService != null && flowService.isRunning()) {
                        flowService.stop(false);
                    }
                    logger.error("Unable to load flow due to: " + e, e);
                    throw new Exception("Unable to load flow due to: " + e); // cannot wrap the exception as they are not defined in a classloader accessible to the caller
                }
            }

            final NarLoader narLoader = new StandardNarLoader(
                    props.getExtensionsWorkingDirectory(),
                    props.getComponentDocumentationWorkingDirectory(),
                    NarClassLoadersHolder.getInstance(),
                    extensionManager,
                    extensionMapping,
                    this);

            narAutoLoader = new NarAutoLoader(props.getNarAutoLoadDirectory(), narLoader);
            narAutoLoader.start();

            // dump the application url after confirming everything started successfully
            dumpUrls();
        } catch (Exception ex) {
            startUpFailure(ex);
        }
    }

    @Override
    public void setExtensionMapping(ExtensionMapping extensionMapping) {

    }

    @Override
    public void setBundles(Bundle systemBundle, Set<Bundle> bundles) {

    }

    @Override
    public void stop() {

    }
}
