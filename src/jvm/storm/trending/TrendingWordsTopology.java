package storm.trending;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Calculates trending words, based on a spout that emits words at varying frequencies, a sliding window
 * bolt which time-buckets term frequencies, and a final bolt which captures the top N words.
 */
public class TrendingWordsTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    final String wordgenSpoutId = "wordgen-spout";
    final String slideBoltId = "slide-bolt";
    final String topnNBoltId = "topn-bolt";

    builder.setSpout(wordgenSpoutId, new WordGeneratorSpout(), 1);
    builder.setBolt(slideBoltId, new SlidingWindowBolt(), 1).fieldsGrouping(wordgenSpoutId, new Fields("word"));
    builder.setBolt(topnNBoltId, new TopNWordsBolt(), 1).globalGrouping(slideBoltId);

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(1);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(100000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
