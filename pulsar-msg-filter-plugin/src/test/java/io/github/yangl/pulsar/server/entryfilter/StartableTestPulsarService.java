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
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This is an internal class used by {@link PulsarTestContext} as the {@link PulsarService} implementation
 * for a "startable" PulsarService. Please see {@link PulsarTestContext} for more details.
 */
class StartableTestPulsarService extends AbstractTestPulsarService {
    
    private final Function<BrokerService, BrokerService> brokerServiceCustomizer;
    
    public StartableTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                      MetadataStoreExtended localMetadataStore,
                                      MetadataStoreExtended configurationMetadataStore,
                                      Compactor compactor,
                                      BrokerInterceptor brokerInterceptor,
                                      BookKeeperClientFactory bookKeeperClientFactory,
                                      Function<BrokerService, BrokerService> brokerServiceCustomizer) {
        super(spyConfig, config, localMetadataStore, configurationMetadataStore, compactor, brokerInterceptor,
                bookKeeperClientFactory);
        this.brokerServiceCustomizer = brokerServiceCustomizer;
    }
    
    @Override
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return brokerServiceCustomizer.apply(super.newBrokerService(pulsar));
    }
    
    @Override
    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> spyConfig.getNamespaceService().spy(NamespaceService.class, this);
    }
}
