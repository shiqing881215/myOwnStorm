package Trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * A class to format the from and to mobile number into a new field 
 *
 */
public class FormatCall extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String fromMobileNumber = tuple.getString(0);
		String toMobileNumber = tuple.getString(1);
		collector.emit(new Values(fromMobileNumber + " - " + toMobileNumber));
	}

}
