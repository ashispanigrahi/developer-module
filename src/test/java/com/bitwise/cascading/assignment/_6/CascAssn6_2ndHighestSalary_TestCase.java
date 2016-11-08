package com.bitwise.cascading.assignment._6;

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
public class CascAssn6_2ndHighestSalary_TestCase {

 CascAssn6_2ndHighestSalary cascAssn6Obj = new CascAssn6_2ndHighestSalary();
 Plunger plunger = new  Plunger();
 
 Data emp_details_CSV_Data = null;
 Pipe emp_details_Pipe_CSV = null;
	
	 @Before
	    public void runFirst(){
		 emp_details_CSV_Data = new DataBuilder(new Fields("emp_id","emp_name","sal","DOJ"))
	                .addTuple("1001","John","4000","12/02/2016")
	                .addTuple("1002","Terry","5000","12/02/2016")
	                .addTuple("1003","Brian","8000","12/02/2016")
	                .addTuple("1004","Ashish","3000","12/02/2016")
	                .build();
	    }
 
	 
	 @Test
	 public void findSecondHighestSalary(){
		 emp_details_Pipe_CSV = plunger.newNamedPipe("emp_details_Pipe_CSV", emp_details_CSV_Data);
	        Pipe OUT_emp_details_Pipe_CSV = cascAssn6Obj.salary2ndHighest(emp_details_Pipe_CSV);

	        Bucket bucket = plunger.newBucket(Fields.ALL, OUT_emp_details_Pipe_CSV);
	        List<Tuple> actual = bucket.result().asTupleList();
	        
	        System.out.println(actual);
		 
	 }
}
