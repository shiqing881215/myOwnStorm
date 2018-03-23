package Trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * A split function to split a string with comma
 *
 */
public class CommaSplit extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		for (String word : tuple.getString(0).split(",")) {
			if (word.length() > 0) {
				collector.emit(new Values(word));
			}
		}
	}

}
