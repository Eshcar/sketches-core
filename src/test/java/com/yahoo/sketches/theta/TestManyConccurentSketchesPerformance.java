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

public class TestManyConccurentSketchesPerformance {

	public final Log LOG = LogFactory.getLog(TestManyConccurentSketchesPerformance.class);
	public Logger logger = Logger.getLogger("MyLog");
	public FileHandler fh;

	private MWMRHeapQuickSelectSketch[] skArray_;

	public TestManyConccurentSketchesPerformance(int sketches) {
		
		skArray_ = new MWMRHeapQuickSelectSketch[sketches];

		UpdateSketchBuilder usb = new UpdateSketchBuilder();
		usb.setFamily(Family.MWMR_QUICKSELECT);

		for (int i = 0; i < sketches; i++) {
			skArray_[i] = ((MWMRHeapQuickSelectSketch) usb.build());
		}

	}
	

	public static void main(String[] args) throws Exception {

		int numOfWriters = Integer.parseInt(args[0]);
		int numOfSketches = Integer.parseInt(args[1]);
		int time = Integer.parseInt(args[2]);
		boolean print = Boolean.parseBoolean(args[3]);

		
		TestManyConccurentSketchesPerformance test = new TestManyConccurentSketchesPerformance(numOfSketches);

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

		if (print) {
			test.logger.info("writers = " + numOfWriters);
		}

		test.runTest(numOfWriters, 0 , numOfSketches , time);

		test.LOG.info("Done!");

		System.exit(0);

	}

	private void runTest(int writersNum, int readersNum, int sketchesNum, int secondsToRun) throws Exception {

		TestContext ctx = new TestContext();
		
		ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(sketchesNum);
		int numOfPreparedAccesses = sketchesNum * 50;

		// OperationsNum ops = new OperationsNum();

		List<WorkerWriterThread> writersList = Lists.newArrayList();
		for (int i = 0; i < writersNum; i++) {
			// TODO generate random patarn;
			
			int[] sketchesAccesPatern = new int[numOfPreparedAccesses];
			
			for (int j = 0; j< numOfPreparedAccesses; j++) {
				sketchesAccesPatern[j] = gen.nextInt();
			}
			
			WorkerWriterThread writer = new WorkerWriterThread(i, writersNum, sketchesAccesPatern, numOfPreparedAccesses);
//			WorkerWriterThread writer = new WorkerWriterThread(i, writersNum, sketchesNum);
			writersList.add(writer);
			ctx.addThread(writer);
		}

		// List<ReaderThread> readersList = Lists.newArrayList();
		// for (int i = 0; i < readersNum; i++) {
		// ReaderThread reader = new ReaderThread();
		// readersList.add(reader);
		// ctx.addThread(reader);
		// }

		ctx.startThreads();
		ctx.waitFor(secondsToRun * 1000);
		ctx.stop();

		// long totalReads = 0;
		long totalWrites = 0;

		for (WorkerWriterThread writer : writersList) {
			totalWrites += writer.operationsNum_;
		}
		logger.info("writeTput = " + ((totalWrites / secondsToRun)) / 1000000 + " millions per second");

		// LOG.info("Read threads:");
		// for (ReaderThread reader : readersList) {
		// totalReads += reader.readOperationsNum_;
		// }
		// logger.info("readTput = " + ((totalReads / secondsToRun)) / 1000000.0 +
		// "millions per second");

		double tmp = 0;
		for (int i = 0; i < sketchesNum; i++) {
			tmp += skArray_[i].getEstimate();

		}
		LOG.info("Estimation = " + tmp);
	}

	public class WorkerWriterThread extends TestThread {

		long operationsNum_ = 0;
		int index_ = 0; 
		int[] accessPattarn_;
		int acessPattaenNum_;
		int numOfPreparedAccesses_;
//		ScrambledZipfianGenerator gen_;
		

		int i_;
		int jump_;

//		public WorkerWriterThread(int id, int jump, int sketches ) {
		public WorkerWriterThread(int id, int jump, int[] sketchesAccesPatern, int  numOfPreparedAccesses) {
			super(null, "WRITER");
			i_ = id;
			jump_ = jump;
			accessPattarn_ = sketchesAccesPatern;
			numOfPreparedAccesses_ = numOfPreparedAccesses;
//			gen_  = new ScrambledZipfianGenerator(sketches);
			
		}

		@Override
		public void doWork() throws Exception {
			
			
			MWMRHeapQuickSelectSketch sk = skArray_[accessPattarn_[index_]];
			
//			MWMRHeapQuickSelectSketch sk = skArray_[accessPattarn_[0]];
			
			index_++;
			if (index_ == numOfPreparedAccesses_) {
				index_ = 0;
			}
			
//			MWMRHeapQuickSelectSketch sk = skArray_[gen_.nextInt()];
			
			
			WorkerWriterThread thd = (WorkerWriterThread) Thread.currentThread();
			ContextTheta ctx = thd.getThetaContex(sk);
			ctx.update(i_);

			// contex_.update(i_);
			operationsNum_++;
			i_ = i_ + jump_; // make sure all updates are unique.
			
		}

	}

}
