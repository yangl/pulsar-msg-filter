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

import org.mockito.Mockito;

import java.util.UUID;

/**
 * Holds util methods used in test.
 */
public class BrokerTestUtil {
    
    // Generate unique name for different test run.
    public static String newUniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
    
    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy is stub-only which does not record method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgs(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }
    
    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy records method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgsRecordingInvocations(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }
    
    /**
     * Create a Mockito spy that is stub-only which does not record method invocations,
     * thus saving memory but disallowing verification of invocations.
     *
     * @param object to spy on
     * @param <T> type of object
     * @return a spy of the real object
     */
    public static <T> T spyWithoutRecordingInvocations(T object) {
        return Mockito.mock((Class<T>) object.getClass(), Mockito.withSettings()
                .spiedInstance(object)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }
}
