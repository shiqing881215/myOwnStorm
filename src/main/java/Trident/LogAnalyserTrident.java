package Trident;

import java.util.Random;

import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;

/**
 * A Trident Topology Example
 * Reference : https://www.tutorialspoint.com/apache_storm/apache_storm_trident.htm
 * 
 * General Steps :
 * 1. Create a TridentTopology instance
 * 2. Create a Spout instance (no matter you mimic the data stream or it comes from real stuff)
 * 3. Generate the TridentState (The result is the input tuple after a series of operation)
 * 4. Generate the Stream by using the above TridentState
 * 5. Use DRPC to execute different stream
 *
 */
public class LogAnalyserTrident {
	public static void main(String[] args) {
		// First create the TridentTopology instance
		TridentTopology topology = new TridentTopology();
		
		// Create a test sprout instance which contains three fields
		// fromMobileNumber / toMobileNumber / duration
		FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("fromMobileNumber", "toMobileNumber", "duration"));
		
		// Generate the TridentState (Think it as a table or data structure we can query later)
		// Here the result is a Map from number to count as
		// {"1234123401 - 1234123402" : 12, "1234123401 - 1234123403" : 7}
		TridentState callCounts = topology
				.newStream("fixed-batch-spout", testSpout)  // set up the sprout
				.each(new Fields("fromMobileNumber", "toMobileNumber"), new FormatCall(), new Fields("call"))  // create the new field call which is a combination of froma and to number
				.groupBy(new Fields("call"))  // group by this fromMobileNumber-toMobileNumber field which is unique (PK)
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));   // this generate a MapState, key is call and value is count
		
		// Create local DRPC client
		LocalDRPC drpc = new LocalDRPC();
		
		// Setup two topologies based on the the above TridentState
		// You can think this as you already have a database table (TridentState)
		// Then you are exposing some api (set up different topologies) upon that table
		// Or when you use TridentState, think it as some data structure, here it's a Map
		// so when you use it, treat it as give a key to a map and get some value back
		
		// First topology - given the key and get the count value
		topology.newDRPCStream("call_count", drpc)
			.stateQuery(callCounts, new Fields("args"), new MapGet(), new Fields("count"));
		
		// Second topology - given list of keys and get the total count of these number
		topology.newDRPCStream("multi_call_count", drpc)
			.each(new Fields("args"), new CommaSplit(), new Fields("call"))   // first split the multiple call numbers
			.groupBy(new Fields("call"))   // then group the different call numbers
			.stateQuery(callCounts, new Fields("call"), new MapGet(), new Fields("count"))    // query the TridentState to get count for each call
			.each(new Fields("call", "count"), new Debug())    // add debug line to show what is the value of call and count
			.each(new Fields("count"), new FilterNull())    // filter the word doesn't have a count
			.aggregate(new Fields("count"), new Sum(), new Fields("sum"));   // finally do a sum on all the count field
		
		// Set config and subimit the topology to storm
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident", config, topology.build());
		
		// Random generate some data and let sprout emit them
		Random randomGenerator = new Random();
		int idx = 0;
		while (idx < 10) {
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123402", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123403", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123404", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123402", "1234123403", randomGenerator.nextInt(60))));
			idx = idx + 1;
		}
		
		// Everything is set up, now it's fun time
		System.out.println("DRPC : Query starts");
		// Test two topology separately (think test two api exposed from the table)
		// "1234123401 - 1234123402" is the key format
		System.out.println(drpc.execute("call_count", "1234123401 - 1234123402"));
		System.out.println(drpc.execute("multi_call_count", "1234123401 - 1234123402,1234123401 - 1234123403"));
		System.out.println("DRPC : Query ends");
		
		// Clean up
		cluster.shutdown();
		drpc.shutdown();
	}
}
