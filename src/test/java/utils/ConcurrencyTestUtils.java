package utils;

import java.util.HashSet;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConcurrencyTestUtils {

	private static final Log LOG = LogFactory.getLog(ConcurrencyTestUtils.class);

	// public static class MyResult {
	//
	// public int readsNum_;
	// public int writesNum_;
	// public int readLatency_;
	// public int writesTPut_;
	//
	// }

	public static class TestContext {

		private Throwable err_ = null;
//		private boolean readAlways_ = false;
//		private int readNum_ = 0;
		private Set<TestThread> testThreads_ = new HashSet<TestThread>();
//		private boolean stopped_ = false;
//		private boolean start_ = false;
		// private Set<MyResult> results_ = new HashSet<MyResult>();

		public void addThread(TestThread t) {
			testThreads_.add(t);
		}

		public void startThreads() {
			for (TestThread t : testThreads_) {
				t.start();
			}
			
			
			for (TestThread t : testThreads_) {
				t.startThread();
			}
		}

		private synchronized void checkException() throws Exception {
			if (err_ != null) {
				throw new RuntimeException("Deferred", err_);
			}
		}

		public synchronized void threadFailed(Throwable t) {
			if (err_ == null)
				err_ = t;
			LOG.error("Failed!", err_);
			notify();
		}

//		public synchronized void needToRead() {
//			readNum_++;
//		}

		// public synchronized void addResult(MyResult res) {
		// results_.add(res);
		// }

		public void stop() throws Exception {
//			synchronized (this) {
//				stopped_ = true;
//			}
			
			for (TestThread t : testThreads_) {
				t.stopThread();
			}

			for (TestThread t : testThreads_) {
				t.join();
			}
			checkException();
		}

		public void waitFor(long millis) throws Exception {
			long endTime = System.currentTimeMillis() + millis;
			while (true) {
				long left = endTime - System.currentTimeMillis();
				if (left <= 0)
					break;
				// synchronized (this) {
				// checkException();
				// wait(left);
				// }
			}
		}

//		public boolean isReadAlways() {
//			return readAlways_;
//		}
//
//		public void setReadAlways() {
//			readAlways_ = true;
//		}
//
//		public int getReadNum() {
//			return readNum_;
//		}
//
//		public void incrementReadNum() {
//			readNum_++;
//		}

	}

	/**
	 * A thread that can be added to a test context, and properly passes exceptions
	 * through.
	 */
	public static abstract class TestThread extends Thread {
		// protected final TestContext ctx_;
		private AtomicBoolean stop_ = new AtomicBoolean(false);
		private AtomicBoolean start_ = new AtomicBoolean(false);
		private boolean affinity_ = false;

		public boolean getAffinity_() {
			return affinity_;
		}

		public void setAffinity_(boolean setAffinity) {
			this.affinity_ = setAffinity;
		}

		// public TestThread(TestContext ctx) {
		public TestThread() {
//			this.ctx_ = ctx;
		}

		public void run() {
			
//			AffinityLock.setAffinity (1L << n);

			 while (!start_.get()) {}

			try {
				while (!stop_.get()) {

					doWork();
				}
			} catch (Throwable t) {
				
//				throw new RuntimeException
				
//				ctx_.threadFailed(t);
			}

		}

		public void stopThread() {
			stop_.set(true);
		}
		
		public void startThread() {
			start_.set(true);
		}

		public abstract void doWork() throws Exception;

	}

}
