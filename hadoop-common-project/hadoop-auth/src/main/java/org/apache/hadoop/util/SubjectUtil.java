package org.apache.hadoop.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private()
public class SubjectUtil {
	private static final MethodHandle CALL_AS = lookupCallAs();
	private static final MethodHandle CURRENT = lookupCurrent();

	private SubjectUtil() {
	}

	private static MethodHandle lookupCallAs() {
		MethodHandles.Lookup lookup = MethodHandles.lookup();
		try {
			try {
				// Subject.doAs() is deprecated for removal and replaced by Subject.callAs().
				// Lookup first the new API, since for Java versions where both exist, the
				// new API delegates to the old API (for example Java 18, 19 and 20).
				// Otherwise (Java 17), lookup the old API.
				return lookup.findStatic(Subject.class, "callAs",
						MethodType.methodType(Object.class, Subject.class, Callable.class));
			} catch (NoSuchMethodException x) {
				try {
					// Lookup the old API.
					MethodType oldSignature = MethodType.methodType(Object.class, Subject.class,
							PrivilegedExceptionAction.class);
					MethodHandle doAs = lookup.findStatic(Subject.class, "doAs", oldSignature);
					// Convert the Callable used in the new API to the PrivilegedAction used in the
					// old
					// API.
					MethodType convertSignature = MethodType.methodType(PrivilegedExceptionAction.class,
							Callable.class);
					MethodHandle converter = lookup.findStatic(SubjectUtil.class, "callableToPrivilegedExceptionAction",
							convertSignature);
					return MethodHandles.filterArguments(doAs, 1, converter);
				} catch (NoSuchMethodException e) {
					throw new AssertionError(e);
				}
			}
		} catch (IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	private static MethodHandle lookupCurrent() {
		MethodHandles.Lookup lookup = MethodHandles.lookup();
		try {
			// Subject.getSubject(AccessControlContext) is deprecated for removal and
			// replaced by
			// Subject.current().
			// Lookup first the new API, since for Java versions where both exists, the
			// new API delegates to the old API (for example Java 18, 19 and 20).
			// Otherwise (Java 17), lookup the old API.
			return lookup.findStatic(Subject.class, "current", MethodType.methodType(Subject.class));
		} catch (NoSuchMethodException e) {
			MethodHandle getContext = lookupGetContext();
			MethodHandle getSubject = lookupGetSubject();
			return MethodHandles.filterReturnValue(getContext, getSubject);
		} catch (IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	private static MethodHandle lookupGetSubject() {
		MethodHandles.Lookup lookup = MethodHandles.lookup();
		try {
			Class<?> contextklass = ClassLoader.getSystemClassLoader().loadClass("java.security.AccessControlContext");
			return lookup.findStatic(Subject.class, "getSubject", MethodType.methodType(Subject.class, contextklass));
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	private static MethodHandle lookupGetContext() {
		try {
			// Use reflection to work with Java versions that have and don't have
			// AccessController.
			Class<?> controllerKlass = ClassLoader.getSystemClassLoader().loadClass("java.security.AccessController");
			Class<?> contextklass = ClassLoader.getSystemClassLoader().loadClass("java.security.AccessControlContext");

			MethodHandles.Lookup lookup = MethodHandles.lookup();
			return lookup.findStatic(controllerKlass, "getContext", MethodType.methodType(contextklass));
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Maps to Subject.callAs() if available, otherwise maps to Subject.doAs()
	 * 
	 * @param subject the subject this action runs as
	 * @param action  the action to run
	 * @return the result of the action
	 * @param <T> the type of the result
	 * @throws CompletionException
	 */
	public static <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
		try {
			return (T) CALL_AS.invoke(subject, action);
		} catch (PrivilegedActionException e) {
			throw new CompletionException(e.getCause());
		} catch (Throwable t) {
			throw sneakyThrow(t);
		}
	}

	/**
	 * Maps action to a Callable, and delegates to callAs(). On older JVMs, the
	 * action may be double wrapped (into Callable, and then back into
	 * PrivilegedAction).
	 * 
	 * @param subject the subject this action runs as
	 * @param action action  the action to run
	 * @return the result of the action
	 */
	public static <T> T doAs(Subject subject, PrivilegedAction<T> action) {
		return callAs(subject, privilegedActionToCallable(action));
	}

	 /**
   * Maps action to a Callable, and delegates to callAs(). On older JVMs, the
   * action may be double wrapped (into Callable, and then back into
   * PrivilegedAction).
   * 
   * @param subject the subject this action runs as
   * @param action action  the action to run
   * @return the result of the action
   */
	public static <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
		try {
			return callAs(subject, privilegedExceptionActionToCallable(action));
		} catch (CompletionException ce) {
			try {
				Exception cause = (Exception) (ce.getCause());
				throw new PrivilegedActionException(cause);
			} catch (ClassCastException castException) {
				// This should never happen, as PrivilegedExceptionAction should not wrap
				// non-checked exceptions
				throw new PrivilegedActionException(new UndeclaredThrowableException(ce.getCause()));
			}
		}
	}

	/**
	 * Maps to Subject.currect() is available, otherwise maps to
	 * Subject.getSubject()
	 * 
	 * @return the current subject
	 */
	public static Subject current() {
		try {
			return (Subject) CURRENT.invoke();
		} catch (Throwable t) {
			throw sneakyThrow(t);
		}
	}

	@SuppressWarnings("unused")
	private static <T> PrivilegedExceptionAction<T> callableToPrivilegedExceptionAction(Callable<T> callable) {
		return callable::call;
	}

	private static <T> Callable<T> privilegedExceptionActionToCallable(PrivilegedExceptionAction<T> action) {
		return action::run;
	}

	private static <T> Callable<T> privilegedActionToCallable(PrivilegedAction<T> action) {
		return action::run;
	}

	@SuppressWarnings("unchecked")
	private static <E extends Throwable> RuntimeException sneakyThrow(Throwable e) throws E {
		throw (E) e;
	}
}
