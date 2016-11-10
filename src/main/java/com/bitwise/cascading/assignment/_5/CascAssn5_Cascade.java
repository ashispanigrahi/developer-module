package com.bitwise.cascading.assignment._5;

import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class CascAssn5_Cascade {
	public Pipe acctountBalanceLess500(Pipe Input_Pipe) {

		Not less500 = new Not(new balanceAmountCompare());
		Pipe less500Pipe = new Each(Input_Pipe, less500);

		return less500Pipe;

	}

	public Pipe acctountBalanceMore500(Pipe Input_Pipe) {

		Input_Pipe = new Each(Input_Pipe, new balanceAmountCompare());

		return Input_Pipe;

	}

	public static void main(String[] args) {
		String sourcePath = args[0];

		String lessThan500Path = args[1];

		String moreThan500Path = args[2];

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, CascAssn5_Cascade.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);

		Fields accountfields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance");
		Tap<?, ?, ?> sourceTap = new FileTap(new TextDelimited(accountfields,
				true, ","), sourcePath);
		Pipe accountpipe = new Pipe("accountpipe");

		Tap<?, ?, ?> lessThan500Tap = new FileTap(new TextDelimited(
				accountfields, true, ","), lessThan500Path);
		Pipe lessThan500Pipe = new Pipe("lessThan500Pipe");

		Tap<?, ?, ?> moreThan500Tap = new FileTap(new TextDelimited(
				accountfields, true, ","), moreThan500Path);
		Pipe moreThan500Pipe = new Pipe("moreThan500Pipe");

		CascAssn5_Cascade casObj = new CascAssn5_Cascade();

		Pipe lessThan500OutputPipe = casObj
				.acctountBalanceLess500(lessThan500Pipe);
		Pipe moreThan500OutputPipe = casObj
				.acctountBalanceMore500(moreThan500Pipe);

		FlowDef flowDef = FlowDef.flowDef().addSource(accountpipe, sourceTap)
				.addTailSink(lessThan500OutputPipe, lessThan500Tap);

		FlowDef flowDef1 = FlowDef.flowDef().addSource(accountpipe, sourceTap)
				.addTailSink(moreThan500OutputPipe, moreThan500Tap);

		Flow<?> flow1 = localFlowConnector.connect(flowDef);
		Flow<?> flow2 = localFlowConnector.connect(flowDef1);

		CascadeConnector cascadeConnector = new CascadeConnector();
		Cascade cascade = cascadeConnector.connect(flow1, flow2);
		cascade.complete();

	}
}

@SuppressWarnings("rawtypes")
class balanceAmountCompare extends BaseOperation implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public balanceAmountCompare() {
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall call) {
		TupleEntry tuple = call.getArguments();

		return tuple.getLong("Account_Balance") <= 500;
	}

}