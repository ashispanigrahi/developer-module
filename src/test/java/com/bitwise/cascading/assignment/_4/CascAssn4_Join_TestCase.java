package com.bitwise.cascading.assignment._4;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.List;
import org.junit.Before;



import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CascAssn4_Join_TestCase {
	
	CascAssn4_Join casAssn4Obj = new CascAssn4_Join();
	Plunger plunger = new Plunger();
	
	Data customer_CSV_Data = null;
	Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;
    Pipe customer_Pipe_CSV = null;
    
	@Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("accouno_tran","transaction_date","transaction_amount"))
                .addTuple("1003","12/02/2016","4000")
                .addTuple("1004","12/12/2016","5000")
                .build();
        
        customer_CSV_Data = new DataBuilder(new Fields("account_no","name","dob"))
        .addTuple("1001","John","12/02/1016")
        .addTuple("1002","Baina","12/12/1016")
        .addTuple("1003","Madhia","12/12/1016")
        .addTuple("1004","Papua","12/09/1016").build();
        
    }
	
	@Test
	public void findCustomerWhoMadeTransactions(){
		customer_Pipe_CSV = plunger.newNamedPipe("customer_Pipe_CSV", customer_CSV_Data);
		tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = casAssn4Obj.joinFiles(customer_Pipe_CSV, tran_details_Pipe_CSV);
        
        Bucket bucket = plunger.newBucket(Fields.ALL, OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();
        
        System.out.println(actual);
        
        assertEquals(actual.size(),2);

        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"Madhia");
        assertEquals(actual.get(0).getString(2),"12/12/1016");
        
        assertEquals(actual.get(1).getString(0),"1004");
        assertEquals(actual.get(1).getString(1),"Papua");
        assertEquals(actual.get(1).getString(2),"12/09/1016");
		
	}

}
