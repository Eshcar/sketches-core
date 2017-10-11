package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

public class ContexTheta {

	private AtomicBoolean full_ = new AtomicBoolean(false);
	private HeapQuickSelectSketch localSketch_;
	private MWMRHeapQuickSelectSketch shaerdSketch_;

	public ContexTheta(MWMRHeapQuickSelectSketch sketch) {

		shaerdSketch_ = sketch;
		UpdateSketchBuilder usb = new UpdateSketchBuilder();
		localSketch_ = (HeapQuickSelectSketch)usb.build();
		localSketch_.setThetaLong(sketch.getThetaLong());

	}

	public double getEstimate() {
		return shaerdSketch_.getEstimation();
	}

	public UpdateReturnState update(final long datum) {
		
		UpdateReturnState ret = localSketch_.update(datum);
		if ((localSketch_.getRetainedEntries() + 1 > localSketch_.getHashTableThreshold())
				&& localSketch_.getLgArrLongs() > localSketch_.getLgNomLongs()) { //Theta is going to be changed.
			propogateToSharedSketch(datum);
		}
		
		return ret;

	}
	
	private void propogateToSharedSketch(final long dataItem) {
		
		while (full_.get()) {}
		
		HeapCompactOrderedSketch compactSketch = new HeapCompactOrderedSketch(localSketch_);
		full_.set(true);
		
		shaerdSketch_.prpogate(compactSketch, full_);
		localSketch_.reset();
		localSketch_.setThetaLong(shaerdSketch_.getVolatileTheta());
	}

}
