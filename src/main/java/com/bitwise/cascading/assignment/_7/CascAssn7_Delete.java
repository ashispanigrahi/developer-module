package com.bitwise.cascading.assignment._7;

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
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
public class CascAssn7_Delete {
    public Pipe JoinAndFilter(Pipe  empl_details_Pipe_CSV, Pipe dept_details_Pipe_CSV){

//


        return empl_details_Pipe_CSV; // need to enter the name of the pipe that is being returned
    }
    public static void main(String[] args) {


//To Do
    }


    class filterDeptName extends BaseOperation implements Filter {

        public filterDeptName() {
            super(10, Fields.ALL);
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

            return true; //YOU NEED TO REPLACE THE "TRUE" WITH THE BOOLEAN VALUE WHICH WILL BE INFERRED BY THE
                         //APPLIED LOGIC
        }
    }
}