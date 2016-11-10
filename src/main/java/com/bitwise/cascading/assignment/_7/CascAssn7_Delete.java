package com.bitwise.cascading.assignment._7;

import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bitwise.cascading.assignment._3.CascAssn3_Month;
public class CascAssn7_Delete {
    public Pipe JoinAndFilter(Pipe  empl_details_Pipe_CSV, Pipe dept_details_Pipe_CSV){

       Fields empDeptIdField = new Fields("DeptNo");
       
       //dept_details_Pipe_CSV = new Rename(dept_details_Pipe_CSV, new Fields("DeptNo"), new Fields("Dept_No"));
       Fields deptIdField = new Fields("DeptNo");
       
       Fields outputFields = new Fields("EmpId","EmpFirstName","EmpLastName","Gender","Country","EmpSal","DeptNo","Dept_No","DeptName","ManagerNo");
      
       Pipe joinPipe = new CoGroup(empl_details_Pipe_CSV,empDeptIdField,dept_details_Pipe_CSV,deptIdField,outputFields, new InnerJoin());

       
       joinPipe = new Each(joinPipe , new filterDeptName());
       joinPipe = new Discard(joinPipe, new Fields("Dept_No","DeptName","ManagerNo"));

       return joinPipe; // need to enter the name of the pipe that is being returned
    }
    public static void main(String[] args) {

    	Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn3_Month.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);
		
        String  empPath = args[0];
        Fields employeeFields = new Fields("EmpId","EmpFirstName","EmpLastName","Gender","Country","EmpSal","DeptNo");
        Tap<?, ?, ?> employeeTap = new FileTap(new TextDelimited(employeeFields,true,","),empPath);
        Pipe emp_details_Pipe = new Pipe("empPipe");
        
        String  deptPath = args[1];
        Fields deptFields = new Fields("DeptNo","DeptName","ManagerNo");
        Tap<?, ?, ?> deptTap = new FileTap(new TextDelimited(deptFields,true,","),deptPath);
        Pipe dept_details_Pipe = new Pipe("deptPipe");
        
        String sinkPath = args[2];
        Fields sinkFields = new Fields("EmpId","EmpFirstName","EmpLastName","Gender","Country","EmpSal","DeptNo");
        Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(sinkFields,true,","),sinkPath);
       

        
        CascAssn7_Delete cascAssn7Obj = new CascAssn7_Delete();
        Pipe outputPipe = cascAssn7Obj.JoinAndFilter(emp_details_Pipe, dept_details_Pipe);
         
        
        FlowDef flowDef = FlowDef.flowDef().addSource(emp_details_Pipe, employeeTap)
        		.addSource(dept_details_Pipe, deptTap)
				.addTailSink(outputPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();
        

    }


    @SuppressWarnings("rawtypes")
	class filterDeptName extends BaseOperation implements Filter {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public filterDeptName() {
            super(7, Fields.ALL);
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        	TupleEntry tuples = filterCall.getArguments();
        	String deptName = tuples.getString("DeptName");
        	
        	return (deptName.toLowerCase().contains("sales")); //YOU NEED TO REPLACE THE "TRUE" WITH THE BOOLEAN VALUE WHICH WILL BE INFERRED BY THE
                         //APPLIED LOGIC
        }
    }
}