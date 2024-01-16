/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.github.yangl.pulsar.server.entryfilter;

import io.netty.channel.EventLoopGroup;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.yangl.pulsar.server.entryfilter.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is an internal class used by {@link PulsarTestContext} as the {@link PulsarService} implementation
 * for a "non-startable" PulsarService. Please see {@link PulsarTestContext} for more details.
 */
class NonStartableTestPulsarService extends AbstractTestPulsarService {
    
    private final NamespaceService namespaceService;
    
    public NonStartableTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                         MetadataStoreExtended localMetadataStore,
                                         MetadataStoreExtended configurationMetadataStore,
                                         Compactor compactor, BrokerInterceptor brokerInterceptor,
                                         BookKeeperClientFactory bookKeeperClientFactory,
                                         PulsarResources pulsarResources,
                                         ManagedLedgerStorage managedLedgerClientFactory,
                                         Function<BrokerService, BrokerService> brokerServiceCustomizer) {
        super(spyConfig, config, localMetadataStore, configurationMetadataStore, compactor, brokerInterceptor,
                bookKeeperClientFactory);
        setPulsarResources(pulsarResources);
        setManagedLedgerClientFactory(managedLedgerClientFactory);
        try {
            setBrokerService(brokerServiceCustomizer.apply(
                    spyConfig.getBrokerService().spy(TestBrokerService.class, this, getIoEventLoopGroup())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setSchemaRegistryService(spyWithClassAndConstructorArgs(DefaultSchemaRegistryService.class));
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        setClient(mockClient);
        this.namespaceService = mock(NamespaceService.class);
        try {
            startNamespaceService();
        } catch (PulsarServerException e) {
            throw new RuntimeException(e);
        }
        if (config.isTransactionCoordinatorEnabled()) {
            try {
                setTransactionBufferProvider(TransactionBufferProvider
                        .newProvider(config.getTransactionBufferProviderClassName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                setTransactionPendingAckStoreProvider(TransactionPendingAckStoreProvider
                        .newProvider(config.getTransactionPendingAckStoreProviderClassName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public void start() throws PulsarServerException {
        throw new UnsupportedOperationException("Cannot start a non-startable TestPulsarService");
    }
    
    @Override
    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> namespaceService;
    }
    
    @Override
    public PulsarClientImpl createClientImpl(ClientConfigurationData clientConf) throws PulsarClientException {
        try {
            return (PulsarClientImpl) getClient();
        } catch (PulsarServerException e) {
            throw new PulsarClientException(e);
        }
    }
    
    @Override
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return getBrokerService();
    }
    
    static class TestBrokerService extends BrokerService {
        
        TestBrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
            super(pulsar, eventLoopGroup);
        }
        
        @Override
        protected CompletableFuture<Map<String, String>> fetchTopicPropertiesAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
    }
    
    static class TestPulsarResources extends PulsarResources {
        
        private final TopicResources topicResources;
        private final NamespaceResources namespaceResources;
        
        public TestPulsarResources(MetadataStore localMetadataStore, MetadataStore configurationMetadataStore,
                                   TopicResources topicResources, NamespaceResources namespaceResources) {
            super(localMetadataStore, configurationMetadataStore);
            this.topicResources = topicResources;
            this.namespaceResources = namespaceResources;
        }
        
        @Override
        public TopicResources getTopicResources() {
            return topicResources;
        }
        
        @Override
        public NamespaceResources getNamespaceResources() {
            return namespaceResources;
        }
    }
}
