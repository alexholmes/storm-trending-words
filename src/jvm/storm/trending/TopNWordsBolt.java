package storm.trending;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Adds incoming words and their counts into an internal array sorted by word counts.
 */
public class TopNWordsBolt extends BaseRichBolt {

  private static final Logger LOG = Logger.getLogger(TopNWordsBolt.class);
  List<WordCount> rankedWords = new ArrayList<WordCount>();
  OutputCollector collector;
  private static final WordCountComparator COMPARATOR = new WordCountComparator();

  public static class WordCount {
    String word;
    long count;

    public WordCount(String word, long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return word + ": " + count;
    }

    @Override
    public boolean equals(Object o) {
      return o != null && o instanceof WordCount && word.equals(((WordCount)o).word);
    }
  }

  public static class WordCountComparator implements Comparator<WordCount> {

    @Override
    public int compare(WordCount left, WordCount right) {
      return left.count == right.count ? 0 : left.count < right.count ? -1 : 1;
    }
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
      collect(tuple);
    }
  }

  private void emit() {
    LOG.info(Util.LOG_CORRELATION_KEY + "Top N: " + rankedWords);
  }

  private void collect(Tuple tuple) {
    String word = (String) tuple.getValue(0);
    long count = (Long) tuple.getValue(1);

    boolean updated = false;
    for(WordCount wc: rankedWords) {
      if (wc.word.equals(word)) {
        wc.count += count;
        updated = true;
        break;
      }
    }

    if (!updated) {
      rankedWords.add(new WordCount(word, count));
    }

    Collections.sort(rankedWords, COMPARATOR);
    Collections.reverse(rankedWords);

    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
    return ret;
  }
}
