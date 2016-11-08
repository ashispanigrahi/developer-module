package com.bitwise.cascading.assignment._7;

import java.awt.BufferCapabilities.FlipContents;
import java.util.Properties;

import com.bitwise.cascading.assignment._3.CascAssn3_Month;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
public class CascAssn7_Delete {
    public Pipe JoinAndFilter(Pipe  empl_details_Pipe_CSV, Pipe dept_details_Pipe_CSV){

       Fields empDeptIdField = new Fields("dept_emp_id");
       Fields deptIdField = new Fields("dept_id");
       
      Fields outputFields = new Fields("emp_id","emp_name","sal","dept_emp_id","dept_id","dept_name");
       
       Pipe joinPipe = new CoGroup(empl_details_Pipe_CSV,empDeptIdField,dept_details_Pipe_CSV,deptIdField,outputFields, new InnerJoin());

       joinPipe = new Each(joinPipe , new filterDeptName());

       return joinPipe; // need to enter the name of the pipe that is being returned
    }
    public static void main(String[] args) {

    	Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn3_Month.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);
		
        String  empPath = args[0];
        Fields employeeFields = new Fields("emp_id","emp_name","sal","dept_emp_id");
        Tap<?, ?, ?> employeeTap = new FileTap(new TextDelimited(employeeFields,true,","),empPath);
        Pipe emp_details_Pipe = new Pipe("empPipe");
        
        String  deptPath = args[1];
        Fields deptFields = new Fields("dept_id","dept_name");
        Tap<?, ?, ?> deptTap = new FileTap(new TextDelimited(deptFields,true,","),deptPath);
        Pipe dept_details_Pipe = new Pipe("deptPipe");
        
        String sinkPath = args[2];
        Fields sinkFields = new Fields("emp_id","emp_name","sal","dept_id");
        Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(sinkFields,true,","),sinkPath);
       

        
        CascAssn7_Delete cascAssn7Obj = new CascAssn7_Delete();
        Pipe outputPipe = cascAssn7Obj.JoinAndFilter(emp_details_Pipe, dept_details_Pipe);
         
        
        FlowDef flowDef = FlowDef.flowDef().addSource(emp_details_Pipe, employeeTap)
        		.addSource(dept_details_Pipe, deptTap)
				.addTailSink(outputPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();
        

    }


    class filterDeptName extends BaseOperation implements Filter {

        public filterDeptName() {
            super(6, Fields.ALL);
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        	TupleEntry tuples = filterCall.getArguments();
        	String deptName = tuples.getString("dept_name");
        	
           
        	
        	return (deptName.contains("sales")); //YOU NEED TO REPLACE THE "TRUE" WITH THE BOOLEAN VALUE WHICH WILL BE INFERRED BY THE
                         //APPLIED LOGIC
        }
    }
}