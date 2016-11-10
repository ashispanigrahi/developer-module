package com.bitwise.cascading.assignment._8;

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

public class CascAssn8_Normalize_TestCase {

    CascAssn8_Normalize cascAssn8Normalize = new CascAssn8_Normalize();

    Plunger plunger = new Plunger();

    Data empl_details_CSV_Data = null;
    Pipe empl_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
    	empl_details_CSV_Data = new DataBuilder(new Fields("EmpId","Q1_Salary","Q2_Salary","Q3_Salary","Q4_Salary"))
                .addTuple("1001","5000","5200","5400","5600")
                .addTuple("1002","6000","6200","6400","6600")
                .addTuple("1003","7000","7200","7400","7600")
                .addTuple("1004","8000","8200","8400","8600")
                .build();
        
    }
    
    @Test
    public void convertTable(){
    	empl_details_Pipe_CSV = plunger.newNamedPipe("emp_details_Pipe_CSV", empl_details_CSV_Data);
        Pipe OUT_emp_details_Pipe_CSV = cascAssn8Normalize.normalizeOperation(empl_details_Pipe_CSV);
        Bucket bucket = plunger.newBucket(Fields.ALL, OUT_emp_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();
        
        assertEquals(actual.size(),4);

        assertEquals(actual.get(0).getString(0),"1001");
        assertEquals(actual.get(0).getString(1),"Q1");
    	
    }


}
