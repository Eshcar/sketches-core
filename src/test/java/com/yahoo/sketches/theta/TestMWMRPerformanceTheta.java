package com.yahoo.sketches.theta;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.yahoo.sketches.Family;

import utils.ConcurrencyTestUtils.TestContext;
import utils.ConcurrencyTestUtils.TestThread;

import com.yahoo.sketches.theta.UpdateSketchBuilder;
import com.yahoo.sketches.theta.ContextTheta;

public class TestMWMRPerformanceTheta {

	private MWMRHeapQuickSelectSketch gadget_;
	public final Log LOG = LogFactory.getLog(TestMWMRPerformanceTheta.class);
	public Logger logger = Logger.getLogger("MyLog");
	public FileHandler fh;

	public static void main(String[] args) throws Exception {

		TestMWMRPerformanceTheta test = new TestMWMRPerformanceTheta();

		try {

			// This block configure the logger with handler and formatter
			test.fh = new FileHandler("/home/sashas/sketches/sketches-core/testlog", true);
			test.logger.addHandler(test.fh);
			SimpleFormatter formatter = new SimpleFormatter();
			test.fh.setFormatter(formatter);

			// the following statement is used to log any messages

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		int writers = Integer.parseInt(args[0]);
		int time = Integer.parseInt(args[1]);
		boolean print = Boolean.parseBoolean(args[2]);

		if (print) {
			test.logger.info("writers = " + writers);
		}

		test.setUp();
		test.runTest(writers, 1, time);
		// test.prtintDebug(time);
		// test.clean();

		test.LOG.info("Done!");

		System.exit(0);

	}

	// void prtintDebug(int time) {
	// LOG.info("debug = " + (gadget_.getDebug_() / (time * 1000000)) + " millions
	// per second");
	// }

	// public void clean() {
	// gadget_.clean();
	// }

	public void setUp() throws Exception {

		UpdateSketchBuilder usb = new UpdateSketchBuilder();
		usb.setFamily(Family.MWMR_QUICKSELECT);
		gadget_ = (MWMRHeapQuickSelectSketch) usb.build();
		LOG.info("=============================================MWMR_THETA===========================================");

		ContextTheta ctx = new ContextTheta(gadget_);

		for (long i = 0; i < 10000000; i++) {
			ctx.update(i);
		}

		// contex_ = new Contex(gadget_);
		// LOG.info(contex_.getQuantile(0.5));
		//
		// for (double i = 1; i < 10000000; i++) {
		// contex_.update(i);
		// }
		//
		// LOG.info(contex_.getQuantile(0.5));

	}

	private void runTest(int writersNum, int readersNum, int secondsToRun) throws Exception {

		TestContext ctx = new TestContext();

		// OperationsNum ops = new OperationsNum();

		List<WriterThread> writersList = Lists.newArrayList();
		for (int i = 0; i < writersNum; i++) {
			WriterThread writer = new WriterThread(i, writersNum);
			writersList.add(writer);
			ctx.addThread(writer);
		}

		List<ReaderThread> readersList = Lists.newArrayList();
		for (int i = 0; i < readersNum; i++) {
			ReaderThread reader = new ReaderThread();
			readersList.add(reader);
			ctx.addThread(reader);
		}

		ctx.startThreads();
		ctx.waitFor(secondsToRun * 1000);
		ctx.stop();

		long totalReads = 0;
		long totalWrites = 0;

		for (WriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}
		logger.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

		LOG.info("Read threads:");
		for (ReaderThread reader : readersList) {
			totalReads += reader.readOperationsNum_;
		}
		logger.info("readTput = " + ((totalReads / secondsToRun)) / 1000000.0 + "millions per second");

		LOG.info("Estimation = " + gadget_.getEstimate());

	}

	public class WriterThread extends TestThread {
		long operationsNum_ = 0;
		public final Log LOG = LogFactory.getLog(WriterThread.class);
		private ContextTheta contex_;
		int i_;
		int jump_;

		public WriterThread(int id, int jump) {
			super(null, "WRITER");
			i_ = id;
			jump_ = jump;
			contex_ = new ContextTheta(gadget_);
		}

		@Override
		public void doWork() throws Exception {

			contex_.update(i_);
			operationsNum_++;
			i_ = i_ + jump_;
		}

	}

	public class ReaderThread extends TestThread {

		long readOperationsNum_ = 0;
		private ContextTheta contex_;
		public final Log LOG = LogFactory.getLog(ReaderThread.class);

		public ReaderThread() {
			super(null, "READER");
			contex_ = new ContextTheta(gadget_);

		}

		@Override
		public void doWork() throws Exception {

			contex_.getEstimate();
			readOperationsNum_++;
		}
	}

}
