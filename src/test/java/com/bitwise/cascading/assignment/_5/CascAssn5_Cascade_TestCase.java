package com.bitwise.cascading.assignment._5;

import static org.junit.Assert.*;

import java.util.List;
import org.junit.Before;
import org.junit.Test;


import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CascAssn5_Cascade_TestCase {
	
	CascAssn5_Cascade casAssn5Obj = new CascAssn5_Cascade();
	Plunger plunger = new Plunger();
	
	Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;
    
    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"))
                .addTuple("1001","John","12/02/1016","81111","PUN","4000")
                .addTuple("1002","Brian","12/02/1016","92222","DEL","400")
                .addTuple("1003","Ashish","12/02/1016","79798","BAL","300")
                .addTuple("1004","Terry","12/02/1016","89765","BBS","8000")
                .build();
    }
    
    @Test
    public void findLessThan500Transactions(){
    	tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = casAssn5Obj.acctountBalanceLess500(tran_details_Pipe_CSV);
        Bucket bucket = plunger.newBucket(Fields.ALL, OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();
        
        assertEquals(actual.size(),2);

        assertEquals(actual.get(0).getString(0),"1002");
        assertEquals(actual.get(0).getString(1),"Brian");
        assertEquals(actual.get(0).getString(2),"12/02/1016");
        assertEquals(actual.get(0).getString(3),"92222");
        
        assertEquals(actual.get(1).getString(0),"1003");
        assertEquals(actual.get(1).getString(1),"Ashish");
        assertEquals(actual.get(1).getString(2),"12/02/1016");
        assertEquals(actual.get(1).getString(3),"79798");
    }
    
    
    @Test
    public void findGreaterThan500Transactions(){
    	tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = casAssn5Obj.acctountBalanceMore500(tran_details_Pipe_CSV);
        Bucket bucket = plunger.newBucket(Fields.ALL, OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();
        
        assertEquals(actual.size(),2);
        
        assertEquals(actual.get(0).getString(0),"1001");
        assertEquals(actual.get(0).getString(1),"John");
        assertEquals(actual.get(0).getString(2),"12/02/1016");
        assertEquals(actual.get(0).getString(3),"81111");
        
        assertEquals(actual.get(1).getString(0),"1004");
        assertEquals(actual.get(1).getString(1),"Terry");
        assertEquals(actual.get(1).getString(2),"12/02/1016");
        assertEquals(actual.get(1).getString(3),"89765");
    	
    }

}