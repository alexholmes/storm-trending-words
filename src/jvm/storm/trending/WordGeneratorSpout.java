package storm.trending;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Generates words, periodically changing the frequency at which words are emitted (to better represent
 * change).
 */
public class WordGeneratorSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(SlidingWindowBolt.class);
  private final boolean _isDistributed;
  private SpoutOutputCollector _collector;
  private static final List<String> words = new ArrayList<String>(Arrays.asList(new String[]{"kim", "kourtney", "khloe"}));
  private long lastTopWordsChangeMillis;

  public WordGeneratorSpout() {
    this(true);
  }

  public WordGeneratorSpout(boolean isDistributed) {
    _isDistributed = isDistributed;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  public void close() {

  }

  public void nextTuple() {
    if (timeToChangeTopWords()) {
      changeWordOrdering();
    }
    Utils.sleep(1000);

    emit();
  }

  private void emit() {
    int numEmissions = 2 << words.size();
    for (int i = 0; i < words.size(); i++) {
      for (int x = 0; x < numEmissions; x++) {
        _collector.emit(new Values(words.get(i)));
      }
      numEmissions = numEmissions >> 2;
    }
  }

  private void changeWordOrdering() {
    lastTopWordsChangeMillis = System.currentTimeMillis();
    words.add(words.remove(0));
    LOG.info(String.format(Util.LOG_CORRELATION_KEY + "%s is queen!", words.get(0)));
  }

  private boolean timeToChangeTopWords() {
    return System.currentTimeMillis() - lastTopWordsChangeMillis > TimeUnit.SECONDS.toMillis(9);
  }

  public void ack(Object msgId) {

  }

  public void fail(Object msgId) {

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    if (!_isDistributed) {
      Map<String, Object> ret = new HashMap<String, Object>();
      ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
      return ret;
    } else {
      return null;
    }
  }
}
