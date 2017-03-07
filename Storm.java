import twitter4j.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.conf.ConfigurationBuilder;

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
class TwitterSampleSpout extends BaseRichSpout {
   SpoutOutputCollector _collector;
   LinkedBlockingQueue<Status> queue = null;
   TwitterStream _twitterStream;
		
   String consumerKey;
   String consumerSecret;
   String accessToken;
   String accessTokenSecret;
   String[] keyWords;
		
   public TwitterSampleSpout(String consumerKey, String consumerSecret,
      String accessToken, String accessTokenSecret, String[] keyWords) {
         this.consumerKey = consumerKey;
         this.consumerSecret = consumerSecret;
         this.accessToken = accessToken;
         this.accessTokenSecret = accessTokenSecret;
         this.keyWords = keyWords;
   }
		
   public TwitterSampleSpout() {
      // TODO Auto-generated constructor stub
   }
		
   @Override
   public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
         queue = new LinkedBlockingQueue<Status>(10000);
         _collector = collector;
         StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
               queue.offer(status);
            }
					
            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {}
					
            @Override
            public void onTrackLimitationNotice(int i) {}
					
            @Override
            public void onScrubGeo(long l, long l1) {}
					
            @Override
            public void onException(Exception ex) {}
					
            @Override
            public void onStallWarning(StallWarning arg0) {
               // TODO Auto-generated method stub
            }
         };
				
         ConfigurationBuilder cb = new ConfigurationBuilder();
				
         cb.setDebugEnabled(true)
            .setOAuthConsumerKey(consumerKey)
            .setOAuthConsumerSecret(consumerSecret)
            .setOAuthAccessToken(accessToken)
            .setOAuthAccessTokenSecret(accessTokenSecret);
					
         _twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
         _twitterStream.addListener(listener);
				
         if (keyWords.length == 0) {
            _twitterStream.sample();
         }else {
            FilterQuery query = new FilterQuery().track(keyWords);
            _twitterStream.filter(query);
         }
   }
			
   @Override
   public void nextTuple() {
      Status ret = queue.poll();
				
      if (ret == null) {
         Utils.sleep(10);
      } else {
         _collector.emit(new Values(ret));
      }
   }
			
   @Override
   public void close() {
      _twitterStream.shutdown();
   }
			
   @Override
   public Map<String, Object> getComponentConfiguration() {
      Config ret = new Config();
      ret.setMaxTaskParallelism(10000);
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



class HashtagReaderBolt implements IRichBolt {
   private OutputCollector collector;

   @Override
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
   }

   @Override
   public void execute(Tuple tuple) {
      Status tweet = (Status) tuple.getValueByField("tweet");
      for(HashtagEntity hashtage : tweet.getHashtagEntities()) {
         System.out.println("Hashtag: " + hashtage.getText());
         this.collector.emit(new Values(hashtage.getText()));
      }
   }

   @Override
   public void cleanup() {}

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
	
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}



class HashtagCounterBolt implements IRichBolt {
   Map<String, Integer> counterMap;
   private OutputCollector collector;

   @Override
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.counterMap = new HashMap<String, Integer>();
      this.collector = collector;
   }
   public static long currentTime = System.currentTimeMillis();
   public static long currentCount = 0;

   @Override
   public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      currentCount += 1;

      if(!counterMap.containsKey(key)){
         counterMap.put(key, 1);
      }else{
         Integer c = counterMap.get(key) + 1;
         counterMap.put(key, c);
      }
      if(System.currentTimeMillis() - currentTime >= 60000){
         try {
            String outputFileName = "/home/hduser/result.txt";
            BufferedWriter output = new BufferedWriter(new FileWriter(outputFileName, true));
            for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
               Integer getValue = entry.getValue();
               if(getValue > 0.01 * currentCount){
                  output.write(entry.getKey()+" : " + getValue+" " + 1.0 * getValue / currentCount + " " +currentCount);
                  output.newLine();
                  output.flush();
                  //System.out.println("Result: " + entry.getKey()+" : " + getValue+" " + 1.0*getValue/currentCount+" " +currentCount);
               }
            }
            currentTime = System.currentTimeMillis();
            this.counterMap = new HashMap<String, Integer>();
            currentCount = 0;
            output.newLine();
            output.close();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
      collector.ack(tuple);
   }

   @Override
   public void cleanup() {
      for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
         System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
      }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
	
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
}



public class Storm {
   public static void main(String[] args) throws Exception{
      String consumerKey = args[0];
      String consumerSecret = args[1];
      String accessToken = args[2];
      String accessTokenSecret = args[3];
		
      String[] arguments = args.clone();
      String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
		
      Config config = new Config();
      config.setDebug(true);
      config.setNumWorkers(12);
      config.setMaxSpoutPending(10000);
      config.setMaxTaskParallelism(10000);
		
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
         consumerSecret, accessToken, accessTokenSecret, keyWords), 1500);

      builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(), 1500)
         .shuffleGrouping("twitter-spout");

      builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
	  .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
			
      StormSubmitter cluster = new StormSubmitter();
      cluster.submitTopology("TwitterHashtagStorm", config,
         builder.createTopology());
   }
}


