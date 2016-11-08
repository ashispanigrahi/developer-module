package com.bitwise.cascading.assignment._8;

import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bitwise.cascading.assignment._3.CascAssn3_Month;
public class CascAssn8_Normalize {
    public Pipe normalizeOperation(Pipe  empl_data_Pipe){

//
    	empl_data_Pipe = new Each(empl_data_Pipe,Fields.ALL, new NormalizeFunction(),Fields.RESULTS);
    	
        return empl_data_Pipe;
    }
    public static void main(String[] args) {
    	Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn3_Month.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);
		
        String  empPath = args[0];
        Fields employeeFields = new Fields("emp_id","Q1Sal","Q2Sal","Q3Sal","Q4Sal");
        Tap<?, ?, ?> employeeTap = new FileTap(new TextDelimited(employeeFields,true,","),empPath);
        Pipe emp_details_Pipe = new Pipe("empPipe");
        
        String outputPath = args[1];
        Fields outputFields = new Fields("emp_id","Quarter","Sal");
        Tap<?, ?, ?> outputTap = new FileTap(new TextDelimited(outputFields,true,","),outputPath);
        
        CascAssn8_Normalize cascAssn8Obj = new CascAssn8_Normalize();
        
        Pipe outputPipe = cascAssn8Obj.normalizeOperation(emp_details_Pipe);
        

		FlowDef flowDef = FlowDef.flowDef()
				.addSource(emp_details_Pipe, employeeTap)
				.addTailSink(outputPipe, outputTap);

		localFlowConnector.connect(flowDef).complete();
    	
    }

    @SuppressWarnings("rawtypes")
	static class NormalizeFunction extends BaseOperation implements Function{

        /**
		 * 
		 */
    	private static final long serialVersionUID = 1L;
    	static final Fields outputFields = new Fields("emp_id","Quarter","Sal");

		public NormalizeFunction() {

			super(4, outputFields);
		}

		@Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry enrty = functionCall.getArguments();
    		Tuple tuple = new Tuple();

    		long q1Sal = enrty.getLong("Q1Sal");
    		long q2Sal = enrty.getLong("Q2Sal");
    		long q3Sal = enrty.getLong("Q3Sal");
    		long q4Sal = enrty.getLong("Q4Sal");
    		
    		
    		tuple.add(enrty.getLong("emp_id"));
    		
    		tuple.add("Q1");
    		
    		tuple.add(q1Sal);
    		
    		
    		functionCall.getOutputCollector().add(tuple);

        }

    }

}
