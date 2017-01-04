package com.yahoo.sketches.sampling;

import java.util.Random;

/**
 * Common utility functions for the sampling family of sketches.
 *
 * @author Jon Malkin
 */
final class SamplingUtil {

  public static final Random rand = new Random();

  private SamplingUtil() {}

  /**
   * Checks if target sampling allocation is more than 50% of max sampling size. If so, returns
   * max sampling size, otherwise passes through the target size.
   *
   * @param maxSize      Maximum allowed reservoir size, as from getK()
   * @param resizeTarget Next size based on a pure ResizeFactor scaling
   * @return <code>(reservoirSize_ &lt; 2*resizeTarget ? reservoirSize_ : resizeTarget)</code>
   */
  static int getAdjustedSize(final int maxSize, final int resizeTarget) {
    if (maxSize - (resizeTarget << 1) < 0L) {
      return maxSize;
    }
    return resizeTarget;
  }

  static double nextDoubleExcludeZero() {
    final double r = rand.nextDouble();
    return r == 0.0 ? nextDoubleExcludeZero() : r;
  }

  static int startingSubMultiple(final int lgTarget, final int lgRf, final int lgMin) {
    return (lgTarget <= lgMin)
            ? lgMin : (lgRf == 0) ? lgTarget
            : (lgTarget - lgMin) % lgRf + lgMin;
  }

  // From: https://stackoverflow.com/a/23574723
  // which is a re-indexed version of Luc Devroye's "Second Waiting Time Method" on page 522 of his
  // text "Non-Uniform Random Variate Generation."
  // Faster acceptance/rejection methods exist, but are more complex to implement. And from the
  // comment thread, may be less numerically stable (although likely an implementation issue).
  static int getBinomial(final int n, final double p) {
    final double logq = Math.log(1.0 - p);
    int k = 0;
    double sum = 0;
    for (;;) {
      sum += Math.log(Math.random()) / (n - k);
      if (sum < logq) {
        return k;
      }
      ++k;
    }
  }
}
