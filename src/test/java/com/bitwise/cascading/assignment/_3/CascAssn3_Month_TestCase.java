package com.bitwise.cascading.assignment._3;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;
public class CascAssn3_Month_TestCase {

    CascAssn3_Month cascMonthObj = new CascAssn3_Month();
	
    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;
    
    
    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("accno", "name", "DOB", "phno", "city",
    			"accntbalance"))
                .addTuple("1003","John","12/02/2016","8149","PUN","5000")
                .addTuple("1004","Terry","12/12/2016","8446","DEL","6000")
                .addTuple("1003","Brian","12/12/2016","1111","BXN","7000")
                .addTuple("1004","Zeni","12/09/2016","1111","BLR","8000")
                .build();
    }
    
    @Test
    public void ExtractMonth() {
    	
    	tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
    	
    	Pipe OUT_tran_details_Pipe_CSV = cascMonthObj.extract_month(tran_details_Pipe_CSV);
    	
    	Bucket bucket = plunger.newBucket(new Fields("accno", "name", "DOB", "phno",
				"city", "accntbalance","month"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();
        
        assertEquals(actual.size(), 4);
        
        System.out.println(actual);
        
        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"John");
        assertEquals(actual.get(0).getString(2),"12/02/2016");
        assertEquals(actual.get(0).getString(3),"8149");
        assertEquals(actual.get(0).getString(4),"PUN");
        assertEquals(actual.get(0).getString(5),"5000");
        assertEquals(actual.get(0).getString(6),"February");
        
        
        assertEquals(actual.get(1).getString(6),"December");
        assertEquals(actual.get(2).getString(6),"December");
        assertEquals(actual.get(3).getString(6),"September");
        
        
        
        
        
    }
	
}
