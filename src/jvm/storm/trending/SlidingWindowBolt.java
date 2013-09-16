package storm.trending;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a sliding window of word counts, which is updated with new words as they come in.
 * Summed counts for each item in the window is also periodically emitted for downstream processing.
 */
public class SlidingWindowBolt extends BaseRichBolt {

  private static final Logger LOG = Logger.getLogger(SlidingWindowBolt.class);

  private static final int NUM_WINDOW_CHUNKS = 3;
  private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 2;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;

  private SlidingWindow<String> slidingWindow;
  private final int windowLengthInSeconds;
  private final int emitFrequencyInSeconds;
  OutputCollector collector;

  public SlidingWindowBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public SlidingWindowBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    slidingWindow = new SlidingWindow<String>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.emitFrequencyInSeconds));
  }

  private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
  }

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    if (Util.isTickTuple(tuple)) {
      emit();
    } else {
      countObjAndAck(tuple);
    }
  }

  private void emit() {
    slidingWindow.logState();
    Map<String, Long> counts = slidingWindow.getCountsThenAdvanceWindow();
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      Object obj = entry.getKey();
      Long count = entry.getValue();
      collector.emit(new Values(obj, count));
    }
  }

  private void countObjAndAck(Tuple tuple) {
    String word = (String) tuple.getValue(0);
    slidingWindow.incrementCount(word);
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return ret;
  }
}
