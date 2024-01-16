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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilter.FilterResult;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.yangl.pulsar.common.MsgFilterConstants.MSG_FILTER_EXPRESSION_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@Slf4j
public class MsgFilterImplTest {
    
    private PulsarTestContext pulsarTestContext;
    private ManagedLedger ledgerMock;
    private ManagedCursorImpl cursorMock;
    private PersistentTopic topic;
    private PersistentSubscription persistentSubscription;
    private Consumer consumerMock;
    private ManagedLedgerConfig managedLedgerConfigMock;
    
    final String successTopicName = "persistent://t/n/successTopic";
    final String subName = "sub1";
    
    private MsgFilterImpl msgFilter;
    
    @BeforeMethod
    public void setup() throws Exception {
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .spyByDefault()
                .configCustomizer(config -> {
                    config.setTransactionCoordinatorEnabled(false);
                })
                .useTestPulsarResources()
                .build();
        
        NamespaceResources namespaceResources = pulsarTestContext.getPulsarResources().getNamespaceResources();
        doReturn(Optional.of(new Policies())).when(namespaceResources)
                .getPoliciesIfCached(any());
        
        ledgerMock = mock(ManagedLedgerImpl.class);
        cursorMock = mock(ManagedCursorImpl.class);
        managedLedgerConfigMock = mock(ManagedLedgerConfig.class);
        doReturn(new ManagedCursorContainer()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(new PositionImpl(1, 50)).when(cursorMock).getMarkDeletedPosition();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();
        doReturn(managedLedgerConfigMock).when(ledgerMock).getConfig();
        doReturn(false).when(managedLedgerConfigMock).isAutoSkipNonRecoverableData();
        
        topic = new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());
        
        consumerMock = mock(Consumer.class);
        
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false);
        
        msgFilter = new MsgFilterImpl();
    }
    
    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }
    
    @Test
    public void testFilterEntry0() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("vvvv");
        
        KeyValue kv3 = new KeyValue();
        kv3.setKey("k3");
        kv3.setValue("false");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2, kv3));
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='false')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.ACCEPT);
    }
    
    @Test
    public void testFilterEntry1() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("vvvv");
        
        KeyValue kv3 = new KeyValue();
        kv3.setKey("k3");
        kv3.setValue("true");
        
        KeyValue kv4 = new KeyValue();
        kv4.setKey("k4");
        kv4.setValue("3.63");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2, kv3, kv4));
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "long(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.ACCEPT);
    }
    
    @Test
    public void testFilterEntry2() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("vvvv");
        
        KeyValue kv3 = new KeyValue();
        kv3.setKey("k3");
        kv3.setValue("false");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2, kv3));
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.REJECT);
    }
    
    @Test
    public void testFilterEntry3() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("3");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1));
        
        // only exec `double(k1)<6` get true, then return
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.ACCEPT);
    }
    
    @Test
    public void testFilterEntry4() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("3");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1));
        
        // only exec `long(k1)<6` get true, then return
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "long(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.ACCEPT);
    }
    
    @Test
    public void testFilterEntry5() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("kkkk");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2));
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.REJECT);
    }
    
    @Test
    public void testFilterEntry6() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("vvvv");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2));
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.REJECT);
    }
    
    @Test
    public void testFilterEntry7() {
        KeyValue kv1 = new KeyValue();
        kv1.setKey("k1");
        kv1.setValue("9");
        
        KeyValue kv2 = new KeyValue();
        kv2.setKey("k2");
        kv2.setValue("vvvv");
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.addAllProperties(List.of(kv1, kv2));
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), EntryFilter.FilterResult.ACCEPT);
    }
    
    @Test
    public void testFilterEntry8() {
        MessageMetadata metadata = new MessageMetadata();
        
        Map<String, String> subscriptionProperties =
                Map.of(MSG_FILTER_EXPRESSION_KEY, "double(k1)<6 || (k2=='vvvv' && k3=='true')");
        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false, subscriptionProperties);
        
        FilterContext context = new FilterContext();
        context.setMsgMetadata(metadata);
        context.setSubscription(persistentSubscription);
        
        Entry entry = EntryImpl.create(1, 1, "hello".getBytes(StandardCharsets.UTF_8));
        
        Assert.assertEquals(msgFilter.filterEntry(entry, context), FilterResult.ACCEPT);
    }
    
    static <T> T spyWithClassAndConstructorArgs(Class<T> classToSpy, Object... args) {
        return Mockito.mock(
                classToSpy, Mockito.withSettings().useConstructor(args).defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }
    
    // Prevent the MockBookKeeper instance from being closed when the broker is restarted
    // within a test
    static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {
        
        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
        }
        
        @Override
        public void close() {
            // no-op
        }
        
        @Override
        public void shutdown() {
            // no-op
        }
        
        public void reallyShutdown() {
            super.shutdown();
        }
    }
}
