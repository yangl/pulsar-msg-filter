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

import lombok.Value;
import org.apache.pulsar.broker.PulsarService;
import org.mockito.Mockito;
import org.mockito.internal.creation.instance.ConstructorInstantiator;

/**
 * Configuration for what kind of Mockito spy to use on collaborator objects
 * of the PulsarService managed by {@link PulsarTestContext}.
 * <p>
 * This configuration is applied using {@link PulsarTestContext.Builder#spyConfig(SpyConfig)} or by
 * calling {@link PulsarTestContext.Builder#spyByDefault()} to enable spying for all configurable collaborator
 * objects.
 * <p>
 * For verifying interactions with Mockito's {@link Mockito#verify(Object)} method, the spy type must be set to
 * {@link SpyType#SPY_ALSO_INVOCATIONS}. This is because the default spy type {@link SpyType#SPY}
 * does not record invocations.
 */
@lombok.Builder(builderClassName = "Builder", toBuilder = true)
@Value
public class SpyConfig {
    
    /**
     * Type of spy to use.
     */
    public enum SpyType {
        
        NONE, // No spy
        SPY, // Spy without recording invocations
        SPY_ALSO_INVOCATIONS; // Spy and record invocations
        
        public <T> T spy(T object) {
            if (object == null) {
                return null;
            }
            switch (this) {
                case NONE:
                    return object;
                case SPY:
                    return BrokerTestUtil.spyWithoutRecordingInvocations(object);
                case SPY_ALSO_INVOCATIONS:
                    return Mockito.spy(object);
                default:
                    throw new UnsupportedOperationException("Unknown spy type: " + this);
            }
        }
        
        public <T> T spy(Class<T> clazz, Object... args) {
            switch (this) {
                case NONE:
                    // Use Mockito's internal class to instantiate the object
                    return new ConstructorInstantiator(false, args).newInstance(clazz);
                case SPY:
                    return BrokerTestUtil.spyWithClassAndConstructorArgs(clazz, args);
                case SPY_ALSO_INVOCATIONS:
                    return BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations(clazz, args);
                default:
                    throw new UnsupportedOperationException("Unknown spy type: " + this);
            }
        }
    }
    
    /**
     * Spy configuration for {@link PulsarTestContext#getPulsarService()}.
     */
    private final SpyType pulsarService;
    /**
     * Spy configuration for {@link PulsarService#getPulsarResources()}.
     */
    private final SpyType pulsarResources;
    /**
     * Spy configuration for {@link PulsarService#getBrokerService()}.
     */
    private final SpyType brokerService;
    /**
     * Spy configuration for {@link PulsarService#getBookKeeperClient()}.
     * In the test context, this is the same as {@link PulsarTestContext#getBookKeeperClient()} .
     * It is a client that wraps an in-memory mock bookkeeper implementation.
     */
    private final SpyType bookKeeperClient;
    /**
     * Spy configuration for {@link PulsarService#getCompactor()}.
     */
    private final SpyType compactor;
    /**
     * Spy configuration for {@link PulsarService#getNamespaceService()}.
     */
    private final SpyType namespaceService;
    
    /**
     * Create a builder for SpyConfig with no spies by default.
     *
     * @return a builder
     */
    public static Builder builder() {
        return builder(SpyType.NONE);
    }
    
    /**
     * Create a builder for SpyConfig with the given spy type for all configurable collaborator objects.
     *
     * @param defaultSpyType the spy type to use for all configurable collaborator objects
     * @return a builder
     */
    public static Builder builder(SpyType defaultSpyType) {
        Builder spyConfigBuilder = new Builder();
        spyConfigBuilder.pulsarService(defaultSpyType);
        spyConfigBuilder.pulsarResources(defaultSpyType);
        spyConfigBuilder.brokerService(defaultSpyType);
        spyConfigBuilder.bookKeeperClient(defaultSpyType);
        spyConfigBuilder.compactor(defaultSpyType);
        spyConfigBuilder.namespaceService(defaultSpyType);
        return spyConfigBuilder;
    }
}
