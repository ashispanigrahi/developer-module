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
        Fields employeeFields = new Fields("EmpId","Q1_Salary","Q2_Salary","Q3_Salary","Q4_Salary");
        Tap<?, ?, ?> employeeTap = new FileTap(new TextDelimited(employeeFields,true,","),empPath);
        Pipe emp_details_Pipe = new Pipe("empPipe");
        
        String outputPath = args[1];
        Fields outputFields = new Fields("EmpId","QuaterNo","Salary");
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
    	static final Fields outputFields = new Fields("EmpId","QuaterNo","Salary");

		public NormalizeFunction() {

			super(4, outputFields);
		}

		@Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry enrty = functionCall.getArguments();
    		Tuple tuple = new Tuple();
    		Tuple tuple1 = new Tuple();
    		Tuple tuple2 = new Tuple();
    		Tuple tuple3 = new Tuple();

    		long q1Sal = enrty.getLong("Q1_Salary");
    		long q2Sal = enrty.getLong("Q2_Salary");
    		long q3Sal = enrty.getLong("Q3_Salary");
    		long q4Sal = enrty.getLong("Q4_Salary");
    		
    		tuple.add(enrty.getLong("EmpId"));
    		tuple.add("1");
    		tuple.add(q1Sal);
    		
    		tuple1.add(enrty.getLong("EmpId"));
    		tuple1.add("2");
    		tuple1.add(q2Sal);
    		
    		tuple2.add(enrty.getLong("EmpId"));
    		tuple2.add("3");
    		tuple2.add(q3Sal);
    		
    		tuple3.add(enrty.getLong("EmpId"));
    		tuple3.add("4");
    		tuple3.add(q4Sal);
    		
    		
    		
    		functionCall.getOutputCollector().add(tuple);
    		functionCall.getOutputCollector().add(tuple1);
    		functionCall.getOutputCollector().add(tuple2);
    		functionCall.getOutputCollector().add(tuple3);

        }

    }

}
