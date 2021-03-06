package com.bitwise.cascading.assignment._4;

import java.util.Properties;

import com.bitwise.cascading.assignment._3.CascAssn3_Month;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
public class CascAssn4_Join {
    public Pipe joinFiles(Pipe  customer_data_Pipe, Pipe transaction_data_Pipe){

    	Fields customerAccountNo = new Fields("Account_Number");
    	Fields transactionAccountNo = new Fields("Account_Number1");
    	
    	Fields outputFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","Account_Number1","Transaction_Type","Transaction_Amount","Transaction_Date");
    	  
    	Pipe  	joinPipe = 
    			new CoGroup(customer_data_Pipe,customerAccountNo,transaction_data_Pipe,transactionAccountNo,outputFields,new InnerJoin());
    	 
    	return joinPipe;

      
    }
    public static void main(String[] args) {
    	String customerPath = args[0];
		String transactionPath = args[1];
		
		String sinkPath = args[2];
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn4_Join.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);

		Fields customerFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number");
		Tap<?, ?, ?> customerTap = new FileTap(new TextDelimited(customerFields, true, ","),
				customerPath);
		Pipe customerPipe = new Pipe("customerpipe");
		
		
		Fields transationFields = new Fields("Account_Number1","Transaction_Type","Transaction_Amount","Transaction_Date");
		Tap<?, ?, ?> transationTap = new FileTap(new TextDelimited(transationFields, true, ","),
				transactionPath);
		Pipe transationPipe = new Pipe("transationpipe");
		
		Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(true, ","),
				sinkPath);
		
		CascAssn4_Join casjoin = new CascAssn4_Join();
		Pipe joinPipe = casjoin.joinFiles(customerPipe, transationPipe);
		
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(customerPipe, customerTap)
				.addSource(transationPipe, transationTap)
				.addTailSink(joinPipe, sinkTap);
		
		localFlowConnector.connect(flowDef).complete();
    }
}
