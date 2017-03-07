import twitter4j.*;
import java.util.regex.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.topology.FailedException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import java.io.*;
import java.io.File;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;



@SuppressWarnings("serial")
class LocalFileSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    TwitterStream _twitterStream;
    BufferedReader reader = null;
    Boolean isLastLine = false;

    public LocalFileSpout() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void open(Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
        _collector = collector;
        try {
            this.reader = new BufferedReader(new FileReader("/home/hduser/tweets.txt"));
        } catch (Exception e) {

        }
    }

    @Override
    public void nextTuple() {
        String tweeter = "";
        try {
            if ((tweeter = this.reader.readLine()) != null) {
                _collector.emit(new Values(tweeter));
            } else {
                if (!isLastLine) {
                    tweeter = "[END]";
                    _collector.emit(new Values(tweeter));
                    isLastLine = true;
                }
            }
        } catch (Exception e) {}
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map < String, Object > getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(100);
        return ret;
    }

    @Override
    public void ack(Object id) {}

    @Override
    public void fail(Object id) {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}




class HashtagReaderBolt extends BaseBasicBolt {
    @Override
    public void prepare(Map conf, TopologyContext context) {
        // do nothing
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tweet = (String) tuple.getValueByField("tweet");
        if (!tweet.equals("[END]")) {
            Pattern HASHTAG_PATTERN = Pattern.compile("(?:^|\\s|[\\p{Punct}&&[^/]])(#[\\p{L}0-9-_]+)");
            Matcher mat = HASHTAG_PATTERN.matcher(tweet);
            List < String > tags = new ArrayList < String > ();
            while (mat.find()) {
                tags.add(mat.group(1));
            }
            for (String hashtag: tags) {
                if (hashtag != null) {
                    collector.emit(new Values(hashtag));
                }
            }
            if (new Random().nextInt(9) == 0) {
                throw new FailedException();
            }
        } else {
            String endTag = "[END]";
            collector.emit(new Values(endTag));
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map < String, Object > getComponentConfiguration() {
        return null;
    }

}



class HashtagCounterBolt extends BaseBasicBolt {
    Map < String, Integer > counterMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        this.counterMap = new HashMap < String, Integer > ();
        // this.collector = collector;
    }
    public static long currentTime = System.currentTimeMillis();
    public static long currentCount = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String key = tuple.getString(0);
        if (key.equals("[END]")) {
            try {
                String outputFileName = "/home/hduser/result.txt";
                BufferedWriter output = new BufferedWriter(new FileWriter(outputFileName, true));
                for (Map.Entry < String, Integer > entry: counterMap.entrySet()) {
                    Integer getValue = entry.getValue();
                    if (getValue > 0.01 * currentCount) {
                        output.write(entry.getKey() + " : " + getValue + " " + 1.0 * getValue / currentCount + " " + currentCount);
                        output.newLine();
                        output.flush();
                    }
                }
                output.write(key);
                output.newLine();
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            currentCount += 1;
            if (!counterMap.containsKey(key)) {
                counterMap.put(key, 1);
            } else {
                Integer c = counterMap.get(key) + 1;
                counterMap.put(key, c);
            }
        }
    }

    @Override
    public void cleanup() {
        for (Map.Entry < String, Integer > entry: counterMap.entrySet()) {
            System.out.println("Result: " + entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map < String, Object > getComponentConfiguration() {
        return null;
    }
}



public class LeastOnceLocalStream {
    public static void main(String[] args) throws Exception {
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
        config.setMaxTaskParallelism(10);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new LocalFileSpout(), 1);

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(), 1)
            .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
            .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        StormSubmitter cluster = new StormSubmitter();
        cluster.submitTopology("TwitterHashtagStorm", config,
            builder.createTopology());
    }
}