package org.janelia.saalfeldlab.n5.s3;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.Filterable;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;

public class IgnoreTestCasesByParameter extends BlockJUnit4ClassRunnerWithParametersFactory {

	private static Filter skipAll = new Filter() {

		@Override public boolean shouldRun(Description description) {

			return false;
		}

		@Override public String describe() {

			return "Backend Tests Not Enabled";
		}
	};
	@Override public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {

		final IgnoreParameter annotation = test.getTestClass().getAnnotation(IgnoreParameter.class);
		final int ignoreIndex = annotation != null ? annotation.index() : -1;
		final int idx = ignoreIndex == -1 ? test.getParameters().size() - 1 : ignoreIndex;

		final Object ignoreTests = test.getParameters().get(idx);
		final Runner runnerForTestWithParameters = super.createRunnerForTestWithParameters(test);
		if (Objects.equals(ignoreTests, true)) {
			if (runnerForTestWithParameters instanceof Filterable) {
				try {
					((Filterable)runnerForTestWithParameters).filter(skipAll);
				} catch (NoTestsRemainException ignored) {
				}
			}
		};
		return runnerForTestWithParameters;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface IgnoreParameter {
		/**
		 * Optional index to specify where the boolean that is used to determine
		 * whether a ParameterizedTest run is skipped or not.
		 *
		 * Default value is -1, which uses the final index of the paremeter list.
		 *
		 * @return index to query for whether to ignore the test cases or not.
		 */
		int index() default -1;
	}
}