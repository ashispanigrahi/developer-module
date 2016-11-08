package com.bitwise.cascading.assignment._2;

import java.io.IOException;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class CascAssn2_FilterRecords {
    public Pipe filterTranTypeCNP(Pipe transactions_Input_Pipe) {
    	Pipe csvPipe = new Pipe("csvPipe",transactions_Input_Pipe);
    	csvPipe = new Each(csvPipe, Fields.ALL, new filterToCompareTranAmtNChargOffAmt());
        return csvPipe;
    }

    public static void main(String[] args) throws IOException {
    	String sourcePath = args[0];
		String sinkPath = args[1];
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, CascAssn2_FilterRecords.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);
		
		Fields transactionFields = new Fields("Account_Number",
				"Transaction_Type", "Transaction_Amount", "Transaction_Date");
		Tap<?, ?, ?> sourceTap = new FileTap(new TextDelimited(transactionFields, true, ","), sourcePath);
		Pipe transactionInPipe = new Pipe("transaction_pipe");
		
		Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(transactionFields, true, ","), sinkPath ,SinkMode.KEEP);
		
		CascAssn2_FilterRecords casFilterRecords = new CascAssn2_FilterRecords();
		Pipe transactionsOutPipe = casFilterRecords.filterTranTypeCNP(transactionInPipe);
		
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(transactionInPipe, sourceTap)
				.addTailSink(transactionsOutPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();
		
    }
}

@SuppressWarnings("rawtypes")
class filterToCompareTranAmtNChargOffAmt extends BaseOperation implements Filter {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public filterToCompareTranAmtNChargOffAmt() {
      
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    	TupleEntry tupleEntry = filterCall.getArguments();
    	String transactionType = tupleEntry.getString("Transaction_Type");
    	  
    	return (transactionType.equals("CNP"));
    }
}
