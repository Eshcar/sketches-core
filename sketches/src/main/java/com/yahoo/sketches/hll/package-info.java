/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * <p>The hll package contains a very compact implementation of Phillipe Flajolet's
 * HLL sketch but with significantly improved error behavior.  If the ONLY use case for sketching is
 * counting uniques and merging, the HLL sketch is the highest performing in terms of accuracy for
 * space consumed.  For large counts, this HLL version will be 16 to 32 times smaller for the same
 * accuracy than the Theta Sketches.</p>
 *
 * <p>However, large data with many dimensions and dimension coordinates are often highly skewed
 * creating a "long-tailed" or power-law distribution of unique values per sketch.
 * In this case a majority of sketches tend to have only a few entries.  It is this long tail of
 * the distribution of sketch sizes that will dominate the overall storage cost for all of the
 * sketches. In this case the size advantage of the HLL will be significantly reduced down to a
 * factor of two to four compared to Theta Sketches. This behavior is strictly a function of the
 * distribution of the input data so it is advisable to understand and measure this phenomenon with
 * your own data.</p>
 *
 * <p>The HLL sketch is not recommended if you anticipate the need of performing set intersection
 * or difference operations with reasonable accuracy,
 * or if you anticipate the need to merge sketches that were constructed with different
 * values of <i>k</i> or <i>Nominal Entries</i>.</p>
 *
 * <p>HLL sketches do not retain any of the hash values of the associated unique identifiers,
 * so if there is any anticipation of a future need to leverage associations with these
 * retained hash values, Theta Sketches would be a better choice.</p>
 *
 * <p>HLL sketches cannot be intermixed or merged in any way with Theta Sketches.
 * </p>
 *
 * <p>This package also contains a space efficient HLL mapping sketch of keys to approximate unique
 * count of identifiers. For example, counting the number of unique users (identifiers) per IP
 * address.
 *
 * <p>In cases where the number of keys is very large, having an individual HLL sketch per key may
 * not be practical. If the distribution of values per key is highly skewed where the vast
 * majority of keys have only a few values then this mapping sketch will make sense as it will be
 * far more space efficient than dedicating individual HLL sketches per key.
 *
 * <p>From our own testing, sketching 100 million IPv4 addresses with such a
 * highly skewed distribution of identifiers per IP uses only 1.4GB of memory. This translates to
 * an average of about 10 bytes per IP allocated to the equivalent of a full k=1024 HLL sketch
 * and provides an RSE of less than 2.5%.  Your results will vary depending on the actual
 * distribution of identifiers per key.
 * @see com.yahoo.sketches.hll.UniqueCountMap
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alex Saydakov
 */
package com.yahoo.sketches.hll;
