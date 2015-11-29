package edu.mit.csail.medg.csv2mysql;

import java.util.Map;
import java.util.TreeMap;
import java.math.BigInteger;

/** A RangeTree is a compact representation of a set of ordered but possibly non-contiguous values.
 * It maintains an ordered tree of nodes, each of which represents a non-overlapping and non-contiguous
 * set of values, which are updated any time a new element is added.  To get a compact representation,
 * we need a dense set of values so that we can tell when two ranges meet and can therefore be merged.
 * This is true for integers, but not strings or floating point values.
 * 
 * Adding a new value to a RangeTree does one of the following:
 * 1. If the value is already present, return false.
 * 2. If the value is less than the low end of the lowest range, then
 *   a. If it is one less, extend that range,
 *   b. Otherwise, add a new range for that value.
 * 3. If the value falls between two ranges,
 *   a. If it is one greater than the top of the range below, extend that,
 *   b. If it is one less than the range above, extend that,
 *   c. Otherwise, add a new range for that value.
 *   Then, if the two ranges now meet, merge them.
 *   
 * We represent the RangeTree as a TreeMap in which each entry has as its key the low end of
 * a range and as its value the high end.
 * 
 * Possible cases of where a new value can fit into existing ranges:
 * 
 *       [flr        ]       [ceil       ]
 *             x
 *                           x
 *                    x
 *                          x
 *                       x                    
 *         
 * 
 * @author psz
 *
 */
public class RangeTree {
	TreeMap<BigInteger,BigInteger> t = null;
	
	public RangeTree() {
		t = new TreeMap<BigInteger,BigInteger>();
	}
	
	public boolean add(BigInteger val) {
		
		// Though somewhat controversial, MySQL allows multiple NULL values in a column
		// that is declared as a UNIQUE KEY.  Therefore, we do too.
		if (val == null) return true;
		
		// Find the ranges below and above val, if any
		Map.Entry<BigInteger,BigInteger> flr = t.floorEntry(val);
		Map.Entry<BigInteger,BigInteger> ceil = t.ceilingEntry(val);
		
		// If val intersects either of these ranges, we have a duplicate
		if (intersectsFloor(val, flr) || intersectsCeiling(val, ceil)) return false;
		// If val is adjacent to one of these ranges, simply extend it
		else if (isJustAbove(val, flr)) {
			t.put(flr.getKey(), val);
			flr = t.floorEntry(val);
		}
		else if (isJustBelow(val, ceil)) {
			t.remove(ceil.getKey());
			t.put(val, ceil.getValue());
		}
		// Otherwise, add a new range
		else {
			t.put(val, val);
			flr = t.floorEntry(val);
			ceil = t.higherEntry(val);
		}
		
		// If extension of a range caused two to meet, merge them.
		if (meet(flr, ceil)) {
			t.remove(ceil.getKey());
			t.put(flr.getKey(), ceil.getValue()); 
		}
		
		return true;
	}
	
	private static boolean intersectsFloor(BigInteger val, Map.Entry<BigInteger,BigInteger> range) {
		return (range != null) && (val.compareTo(range.getValue()) <= 0);
	}
	
	private static boolean intersectsCeiling(BigInteger val, Map.Entry<BigInteger,BigInteger> range) {
		return (range != null) && (val.compareTo(range.getKey()) == 0);
	}

	private static boolean isJustAbove(BigInteger val, Map.Entry<BigInteger,BigInteger> range) {
		return (range != null) && (val.subtract(BigInteger.ONE).compareTo(range.getValue()) == 0);
	}
	
	private static boolean isJustBelow(BigInteger val, Map.Entry<BigInteger, BigInteger> range) {
		return (range != null) && (val.add(BigInteger.ONE).compareTo(range.getKey()) == 0);
	}
	
	private static boolean meet(Map.Entry<BigInteger, BigInteger> low, Map.Entry<BigInteger, BigInteger> high) {
		return (low != null) && (high != null) && (low.getValue().add(BigInteger.ONE).compareTo(high.getKey()) >= 0);
	}
	
	public boolean isEmpty() {
		return t.isEmpty();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		String sep = "";
		for (BigInteger k: t.keySet()) {
			sb.append(sep + "[" + k + "," + t.get(k) + "]");
			sep = " ";
		}
		sb.append("]");
		return sb.toString();
	}
}
