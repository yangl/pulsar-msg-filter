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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * This is a JDK Proxy based handler that wraps an AutoCloseable instance and prevents it from being closed
 * when the close method is invoked.
 * The {@link #reallyClose(AutoCloseable)} method can be used to close the delegate instance.
 */
public class NonClosingProxyHandler implements InvocationHandler {
    
    private final AutoCloseable delegate;
    
    NonClosingProxyHandler(AutoCloseable delegate) {
        this.delegate = delegate;
    }
    
    /**
     * Wraps the given delegate instance with a proxy instance that prevents closing.
     */
    public static <T extends AutoCloseable> T createNonClosingProxy(T delegate, Class<T> interfaceClass) {
        if (isNonClosingProxy(delegate)) {
            return delegate;
        }
        return interfaceClass.cast(Proxy.newProxyInstance(delegate.getClass().getClassLoader(),
                new Class<?>[]{interfaceClass}, new NonClosingProxyHandler(delegate)));
    }
    
    /**
     * Returns true if the given instance is a proxy instance created by
     * {@link #createNonClosingProxy(AutoCloseable, Class)}
     *
     * @param instance proxy instance
     * @return true if the given instance is a proxy instance
     */
    public static boolean isNonClosingProxy(Object instance) {
        return Proxy.isProxyClass(instance.getClass())
                && Proxy.getInvocationHandler(instance) instanceof NonClosingProxyHandler;
    }
    
    /**
     * Returns the delegate instance of the given proxy instance.
     *
     * @param instance proxy instance
     * @return delegate instance
     */
    public static <T extends I, I extends AutoCloseable> I getDelegate(T instance) {
        if (isNonClosingProxy(instance)) {
            return (T) ((NonClosingProxyHandler) Proxy.getInvocationHandler(instance)).getDelegate();
        } else {
            throw new IllegalArgumentException("not a proxy instance with NonClosingProxyHandler");
        }
    }
    
    /**
     * Calls close on the delegate instance of a proxy that was created by
     * {@link #createNonClosingProxy(AutoCloseable, Class)}
     *
     * @param instance instance to close
     * @throws Exception if an error occurs
     */
    public static <T extends AutoCloseable> void reallyClose(T instance) throws Exception {
        if (isNonClosingProxy(instance)) {
            getDelegate(instance).close();
        } else {
            instance.close();
        }
    }
    
    /**
     * Returns the delegate instance.
     */
    public AutoCloseable getDelegate() {
        return delegate;
    }
    
    /**
     * JDK Proxy InvocationHandler implementation.
     * If the method is close, then it is ignored.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close")) {
            return null;
        } else {
            return method.invoke(delegate, args);
        }
    }
}
