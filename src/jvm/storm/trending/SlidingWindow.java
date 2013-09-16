package storm.trending;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Maintains a map of items to time-bucketed counts.
 */
public class SlidingWindow<T> implements Serializable {
  private static final Logger LOG = Logger.getLogger(SlidingWindow.class);

  private static final long serialVersionUID = -2645063988768785810L;

  private final Map<T, long[]> objToCounts = new HashMap<T, long[]>();
  private final int numSlots;
  private int headSlot;
  private int tailSlot;


  public SlidingWindow(int numSlots) {
    if (numSlots < 2) {
      throw new IllegalArgumentException(
          "Window length in slots must be at least two (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void incrementCount(T obj) {
    incrementCount(obj, headSlot);
  }

  public void logState() {
    LOG.info(Util.LOG_CORRELATION_KEY + " WINDOW DUMP");

    for(Map.Entry<T, long[]> entry: objToCounts.entrySet()) {
      logEntry(entry.getKey(), entry.getValue());
    }
  }

  private void logEntry(T key, long[] value) {
    StringBuilder line = new StringBuilder(String.format("%10s ", key.toString()));
    int pos = headSlot;
    long total = 0;
    for(int i = 0; i < numSlots; i++) {
      long val = value[(pos = slotAfter(pos))];
      total += val;
      line.append(String.format("%3s ", String.valueOf(val)));
    }
    line.append(String.format(" (%3s)", String.valueOf(total)));
    LOG.info(Util.LOG_CORRELATION_KEY + "Sliding window counts: " + line);
  }

  /**
   * Return the current (total) counts of all tracked objects, then advance the window.
   * <p/>
   * Whenever this method is called, we consider the counts of the current sliding window to be available to and
   * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
   * objects within the next "chunk" of the sliding window.
   *
   * @return The current (total) counts of all tracked objects.
   */
  public Map<T, Long> getCountsThenAdvanceWindow() {
    Map<T, Long> counts = getCounts();
    wipeZeros();
    wipeSlot(tailSlot);
    advanceHead();
    return counts;
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slotAfter(tailSlot);
  }

  private int slotAfter(int slot) {
    return (slot + 1) % numSlots;
  }


  public void incrementCount(T obj, int slot) {
    long[] counts = objToCounts.get(obj);
    if (counts == null) {
      counts = new long[this.numSlots];
      objToCounts.put(obj, counts);
    }
    counts[slot]++;
  }

  public Map<T, Long> getCounts() {
    Map<T, Long> result = new HashMap<T, Long>();
    for (T obj : objToCounts.keySet()) {
      result.put(obj, computeTotalCount(obj));
    }
    return result;
  }

  private long computeTotalCount(T obj) {
    long[] curr = objToCounts.get(obj);
    long total = 0;
    for (long l : curr) {
      total += l;
    }
    return total;
  }

  public void wipeSlot(int slot) {
    for (T obj : objToCounts.keySet()) {
      resetSlotCountToZero(obj, slot);
    }
  }

  private void resetSlotCountToZero(T obj, int slot) {
    long[] counts = objToCounts.get(obj);
    counts[slot] = 0;
  }

  private boolean shouldBeRemovedFromCounter(T obj) {
    return computeTotalCount(obj) == 0;
  }

  public void wipeZeros() {
    Set<T> objToBeRemoved = new HashSet<T>();
    for (T obj : objToCounts.keySet()) {
      if (shouldBeRemovedFromCounter(obj)) {
        objToBeRemoved.add(obj);
      }
    }
    for (T obj : objToBeRemoved) {
      objToCounts.remove(obj);
    }
  }

}