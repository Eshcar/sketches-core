package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

public class MWMRHeapQuickSelectSketch extends HeapQuickSelectSketch {

	private ExecutorService executorService_ = Executors.newSingleThreadExecutor();
	private volatile double estimation_;
	private volatile long theta_;
	public final Log LOG = LogFactory.getLog(MWMRHeapQuickSelectSketch.class);

	public MWMRHeapQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, int preambleLongs,
			Family family) {
		super(lgNomLongs, seed, p, rf, preambleLongs, family);
		
		final int lgArrLongs = Util.startingSubMultiple(lgNomLongs + 1, rf, MIN_LG_ARR_LONGS);
		setLgArrLongs_(lgArrLongs);
		setHashTableThreshold_(setHashTableThreshold(lgNomLongs, lgArrLongs));
		setCurCount_(0);
		setThetaLong_((long) (p * MAX_THETA_LONG_AS_DOUBLE));
		theta_ = (long) (p * MAX_THETA_LONG_AS_DOUBLE);
		setEmpty_(true); // other flags: bigEndian = readOnly = compact = ordered = false;
		setCache_(new long[1 << lgArrLongs]);
	}

	public void prpogate(final Sketch sketchIn, AtomicBoolean full) {
		BackgroundThetaPropogation job = new BackgroundThetaPropogation(sketchIn, full);
		executorService_.execute(job);
	}

	public double getEstimation() {
		return estimation_;
	}
	
	public long getVolatileTheta() {
		return theta_;
	}

	public class BackgroundThetaPropogation implements Runnable {

		Sketch sketch_;
		AtomicBoolean full_;

		BackgroundThetaPropogation(final Sketch sketchIn, AtomicBoolean full) {
			sketch_ = sketchIn;
			full_ = full;
		}

		@Override
		public void run() {
			assert theta_ <= sketch_.getThetaLong();
			
//			LOG.info("I am working!");
			
			
			final long[] cacheIn = sketch_.getCache(); 
			for (int i = 0; i < sketch_.getRetainedEntries(true); i++) {
				final long hashIn = cacheIn[i];
				if (hashIn >= theta_) {
					break;
				} // "early stop"
				hashUpdate(hashIn); // backdoor update, hash function is bypassed
			}
			
			
			estimation_ = getEstimate();
			theta_ = getThetaLong();
			
			full_.set(false);
			
		}
	}

}
