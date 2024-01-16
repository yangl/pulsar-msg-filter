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
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.util.Map;
import java.util.Optional;

/**
 * A {@link BookKeeperClientFactory} that always returns the same instance of {@link BookKeeper}.
 */
class MockBookKeeperClientFactory implements BookKeeperClientFactory {
    
    private final BookKeeper mockBookKeeper;
    
    MockBookKeeperClientFactory(BookKeeper mockBookKeeper) {
        this.mockBookKeeper = mockBookKeeper;
    }
    
    @Override
    public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties) {
        // Always return the same instance (so that we don't loose the mock BK content on broker restart
        return mockBookKeeper;
    }
    
    @Override
    public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) {
        // Always return the same instance (so that we don't loose the mock BK content on broker restart
        return mockBookKeeper;
    }
    
    @Override
    public void close() {
        // no-op
    }
}
