package com.bitwise.cascading.assignment._1;

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
//import com.bitwise.cascading.assignment._1.CascAssn1_MaxAmount;
public class CascAssn1_MaxAmount_TestCase {
	
	CascAssn1_MaxAmount CascAssn1_MaxAmount_Obj = new CascAssn1_MaxAmount();

    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;
	
	 @Before
	    public void runFirst(){
	        tran_details_CSV_Data = new DataBuilder(new Fields("Account_Number","Transaction_Type","Transaction_Amount","Transaction_Date"))
	                .addTuple("1003","CP","4000","12/02/2016")
	                .addTuple("1004","CP","5000","12/12/2016")
	                .addTuple("1003","CNP","8000","12/12/2016")
	                .addTuple("1004","CNP","3000","12/21/2016")
	                .build();
	    }
	
	@Test
	public void MaxAmount() throws Exception{
		tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn1_MaxAmount_Obj.tranWithMaxAmount(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("Account_Number","max"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),2);

        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"8000");

        assertEquals(actual.get(1).getString(0),"1004");
        assertEquals(actual.get(1).getString(1),"5000");
	}


}
