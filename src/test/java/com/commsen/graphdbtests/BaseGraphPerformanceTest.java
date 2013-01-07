
package com.commsen.graphdbtests;

import org.junit.AfterClass;

public abstract class BaseGraphPerformanceTest {

	public static enum TestType {
		INSERT,
		QUERY
	}
	
	private static final String INSERT_TEST_PATTERN_ROW = "| %15d | %15d | %15d | %15d | %15.2f |\n";

	private static final String INSERT_TEST_PATTERN_ROW_MERGED = "| %-87s |\n";

	private static final String INSERT_TEST_PATTERN_SEPARATOR = "| %15s | %15s | %15s | %15s | %15s |\n";

	private static final String INSERT_TEST_PATTERN_SEPARATOR_MERGED = "|-%15s---%15s---%15s---%15s---%15s-|\n";

	private static final Object[] INSERT_TEST_VALUES_SEPARATOR = new String[] {
		"---------------", "---------------", "---------------", "---------------", "---------------"
	};

	protected static String currentHeader = null;

	protected static TestType currentTestType = null;

	@AfterClass
	public static void printClassFooter() {

		System.out.printf(INSERT_TEST_PATTERN_SEPARATOR_MERGED, INSERT_TEST_VALUES_SEPARATOR);
	}

	protected BaseGraphPerformanceTest(TestType testType, String header) {

		if (!testType.equals(currentTestType) || !header.equals(currentHeader)) {
			currentTestType = testType;
			currentHeader = header;
			printHeader();
		}

	}

	public void printHeader() {

		switch (currentTestType) {
		case INSERT:
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR_MERGED, INSERT_TEST_VALUES_SEPARATOR);
			System.out.printf(INSERT_TEST_PATTERN_ROW_MERGED, currentHeader);
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR_MERGED, INSERT_TEST_VALUES_SEPARATOR);
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR, "documents", "vertices", "edges", "time (ms)", "avg (doc/sec)");
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR, INSERT_TEST_VALUES_SEPARATOR);
			break;

		default:
			throw new UnsupportedOperationException();
		}
	}

	protected void printInsertTime(long time, long v, long e) {

		long docs = v + e;
		Double docsPerSecond = (double) docs / ((double) time / (double) 1000);
		System.out.printf(INSERT_TEST_PATTERN_ROW, docs, v, e, time, docsPerSecond);
	}

}
