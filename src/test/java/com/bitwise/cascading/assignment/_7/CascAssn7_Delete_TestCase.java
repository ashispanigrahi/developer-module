package com.bitwise.cascading.assignment._7;


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

public class CascAssn7_Delete_TestCase {

CascAssn7_Delete cascAssnObj = new CascAssn7_Delete();
Plunger plunger = new Plunger();

Data dept_CSV_Data = null;
Data emp_details_CSV_Data = null;
Pipe emp_details_Pipe_CSV = null;
Pipe dept_Pipe_CSV = null;

@Before
public void runFirst(){
	emp_details_CSV_Data = new DataBuilder(new Fields("EmpId","EmpFirstName","EmpLastName","Gender","Country","EmpSal","DeptNo"))
            .addTuple("1001","John","pani","Male","IND","4000","501")
            .addTuple("1002","Terry","Sahoo","Male","UK","5000","502")
            .addTuple("1003","Brian","BS","Female","USA","8000","502")
            .addTuple("1004","Zeni","Rout","Male","IND","2000","504")
            .build();
    
	dept_CSV_Data = new DataBuilder(new Fields("DeptNo","DeptName","ManagerNo"))
    .addTuple("501","ENTC","Rahul")
    .addTuple("502","sales","Rohin")
    .addTuple("503","sales","Pri")
    .addTuple("504","CIV","Kishr").build();
    
}

@Test
public void deleteDeptHavingSales(){
	emp_details_Pipe_CSV = plunger.newNamedPipe("emp_Pipe_CSV", emp_details_CSV_Data);
	dept_Pipe_CSV = plunger.newNamedPipe("dept_Pipe_CSV", dept_CSV_Data);
    Pipe OUT_emp_details_Pipe_CSV = cascAssnObj.JoinAndFilter(emp_details_Pipe_CSV, dept_Pipe_CSV);
    
    Bucket bucket = plunger.newBucket(Fields.ALL, OUT_emp_details_Pipe_CSV);
    List<Tuple> actual = bucket.result().asTupleList();
    
    assertEquals(actual.size(),2);

    assertEquals(actual.get(0).getString(0),"1001");
    assertEquals(actual.get(0).getString(1),"John");
    assertEquals(actual.get(0).getString(2),"pani");
    assertEquals(actual.get(0).getString(3),"Male");
    assertEquals(actual.get(0).getString(4),"IND");
    assertEquals(actual.get(0).getString(5),"4000");
    assertEquals(actual.get(0).getString(6),"501");
	
}

}
