package com.bitwise.cascading.assignment._1;

import java.io.IOException;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.aggregator.MaxValue;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.GroupBy;
import cascading.property.AppProps;

public class CascAssn1_MaxAmount {

	public Pipe tranWithMaxAmount(Pipe transactions_Input_Pipe) {

		Pipe csvPipe = new Pipe("csvPipe",transactions_Input_Pipe);
		
		
		
        Fields accountIdField = new Fields("Account_Number");
		csvPipe = new GroupBy(csvPipe, accountIdField);
		
		Fields amountField = new Fields("Transaction_Amount");
		csvPipe = new Every(csvPipe, amountField, new MaxValue(), Fields.ALL);
		
		return csvPipe;

	}

	public static void main(String[] args) throws IOException {

		String sourcePath = args[0];
		String sinkPath = args[1];
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, CascAssn1_MaxAmount.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);

		Fields transactionFields = new Fields("Account_Number","Transaction_Type","Transaction_Amount","Transaction_Date");
		Tap<?, ?, ?> sourceTap = new FileTap(new TextDelimited(
				transactionFields, false, ","), sourcePath);
		Pipe transactionPipe = new Pipe("transaction_pipe");

		Fields outputFields = new Fields("Account_Number", "max");
		Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(outputFields,
				false, "\t"), sinkPath, SinkMode.KEEP);

		CascAssn1_MaxAmount casMaxAmount = new CascAssn1_MaxAmount();
		Pipe transactionsOutPipe = casMaxAmount
				.tranWithMaxAmount(transactionPipe);

		FlowDef flowDef = FlowDef.flowDef()
				.addSource(transactionPipe, sourceTap)
				.addTailSink(transactionsOutPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();

	}

}