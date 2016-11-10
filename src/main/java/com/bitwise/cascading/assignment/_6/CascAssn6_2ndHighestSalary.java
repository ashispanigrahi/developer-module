package com.bitwise.cascading.assignment._6;


import java.util.Collections;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bitwise.cascading.assignment._3.CascAssn3_Month;

public class CascAssn6_2ndHighestSalary {
    public Pipe salary2ndHighest(Pipe Input_Pipe){
    	Fields amountField = new Fields("TransAmt");
    	amountField.setComparator("TransAmt", Collections.reverseOrder());
    	Input_Pipe = new GroupBy(Input_Pipe , new Fields("AccNo"),amountField);
    	Input_Pipe = new Each(Input_Pipe, Fields.ALL,new CountFunction(),Fields.ALL);
    	Fields f1 = new Fields(0,4);
    	Input_Pipe = new Each(Input_Pipe, f1,new TwoFilter());
        return  Input_Pipe;
    }

    public static void main(String[] args) {
    	Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn3_Month.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);
		
        String  sourcePath = args[0];
      
        Fields transFields = new Fields("AccNo","TransTyp","TransAmt","TransDate");
        Tap<?, ?, ?> sourceTap = new FileTap(new TextDelimited(transFields, false ,","),sourcePath);
        Pipe sourcePipe = new Pipe("transpipe");
      
        String sinkPath = args[1];
        Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(false ,","),sinkPath);
        // Pipe sinkPipe =new Pipe("sinkpipe");
      
        CascAssn6_2ndHighestSalary cascObj = new CascAssn6_2ndHighestSalary();
        Pipe outputPipe =cascObj.salary2ndHighest(sourcePipe);
      
        FlowDef flowDef = FlowDef.flowDef().addSource(sourcePipe, sourceTap)
				.addTailSink(outputPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();
    }

}

@SuppressWarnings("rawtypes")
class CountFunction extends BaseOperation implements Function {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object start =0;
	int count=1;
	public CountFunction() {
	}

	@Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
     TupleEntry tuple = functionCall.getArguments();
     if(tuple.getObject("AccNo").equals(start))
    	 count= count+1;
     else
    	 count =1;
     
     start = tuple.getObject("AccNo");
     
     Tuple result = new Tuple();
     result.add(count);
     
     functionCall.getOutputCollector().add( result );

    }
}

@SuppressWarnings("rawtypes")
class TwoFilter extends BaseOperation implements Filter {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object start = 0;

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
		TupleEntry tuple = filterCall.getArguments();

		if (tuple.getObject(0).equals(start)
				&& tuple.getObject(1).toString().equals("2")) {
			return false;
		}

		else {
			start = tuple.getObject(0);
			return true;
		}
	}
}
