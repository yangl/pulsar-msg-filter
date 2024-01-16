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

import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.io.IOException;

/**
 * This is an internal class used by {@link PulsarTestContext} as the abstract base class for
 * {@link PulsarService} implementations for a PulsarService instance used in tests.
 * Please see {@link PulsarTestContext} for more details.
 */
abstract class AbstractTestPulsarService extends PulsarService {
    
    protected final SpyConfig spyConfig;
    private boolean compactorExists;
    
    public AbstractTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                     MetadataStoreExtended localMetadataStore,
                                     MetadataStoreExtended configurationMetadataStore, Compactor compactor,
                                     BrokerInterceptor brokerInterceptor,
                                     BookKeeperClientFactory bookKeeperClientFactory) {
        super(config);
        this.spyConfig = spyConfig;
        setLocalMetadataStore(
                NonClosingProxyHandler.createNonClosingProxy(localMetadataStore, MetadataStoreExtended.class));
        setConfigurationMetadataStore(
                NonClosingProxyHandler.createNonClosingProxy(configurationMetadataStore, MetadataStoreExtended.class));
        setCompactor(compactor);
        setBrokerInterceptor(brokerInterceptor);
        setBkClientFactory(bookKeeperClientFactory);
    }
    
    @Override
    public MetadataStore createConfigurationMetadataStore(PulsarMetadataEventSynchronizer synchronizer) throws MetadataStoreException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(
                    ((MetadataStoreExtended) getConfigurationMetadataStore())::handleMetadataEvent);
        }
        return getConfigurationMetadataStore();
    }
    
    @Override
    public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer) throws MetadataStoreException, PulsarServerException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(
                    getLocalMetadataStore()::handleMetadataEvent);
        }
        return getLocalMetadataStore();
    }
    
    @Override
    protected void setCompactor(Compactor compactor) {
        if (compactor != null) {
            compactorExists = true;
        }
        super.setCompactor(compactor);
    }
    
    @Override
    public Compactor newCompactor() throws PulsarServerException {
        if (compactorExists) {
            return getCompactor();
        } else {
            return spyConfig.getCompactor().spy(super.newCompactor());
        }
    }
    
    @Override
    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return getBkClientFactory();
    }
    
    @Override
    protected BrokerInterceptor newBrokerInterceptor() throws IOException {
        return getBrokerInterceptor() != null ? getBrokerInterceptor() : super.newBrokerInterceptor();
    }
}
