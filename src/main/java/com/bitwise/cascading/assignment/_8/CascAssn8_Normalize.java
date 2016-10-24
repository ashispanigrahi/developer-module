package com.bitwise.cascading.assignment._8;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
public class CascAssn8_Normalize {
    public Pipe normalizeOperation(Pipe  empl_data_Pipe){

//
        return empl_data_Pipe;
    }
    public static void main(String[] args) {

//To Do
    }

    class NormalizeFunction extends BaseOperation implements Function{


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            // TODO Auto-generated method stub

        }

    }

}
