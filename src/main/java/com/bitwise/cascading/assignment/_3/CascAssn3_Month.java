package com.bitwise.cascading.assignment._3;

import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CascAssn3_Month {
	public Pipe extract_month(Pipe inp_Pipe) {
		Pipe outPipe = new Pipe("outputpipe", inp_Pipe);

		outPipe = new Each(outPipe, Fields.ALL, new date_to_month(),
				Fields.RESULTS);

		return outPipe;
	}

	public static void main(String[] args) {

		String sourcePath = args[0];
		String sinkPath = args[1];

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				CascAssn3_Month.class);
		FlowConnector localFlowConnector = new LocalFlowConnector(properties);

		Fields inputFields = new Fields("accno", "name", "DOB", "phno", "city",
				"accntbalance");
		Tap<?, ?, ?> sourceTap = new FileTap(new TextDelimited(inputFields, true, ","),
				sourcePath);
		Pipe inputPipe = new Pipe("inputpipe");

		Fields outputFields = new Fields("accno", "name", "DOB", "phno",
				"city", "accntbalance", "month");
		Tap<?, ?, ?> sinkTap = new FileTap(new TextDelimited(outputFields, true, ","),
				sinkPath);

		CascAssn3_Month casMonth = new CascAssn3_Month();

		Pipe outputPipe = casMonth.extract_month(inputPipe);

		FlowDef flowDef = FlowDef.flowDef().addSource(inputPipe, sourceTap)
				.addTailSink(outputPipe, sinkTap);

		localFlowConnector.connect(flowDef).complete();

	}
}

@SuppressWarnings("rawtypes")
class date_to_month extends BaseOperation implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Fields outputFields = new Fields("accno", "name", "DOB", "phno",
			"city", "accntbalance", "month");

	public date_to_month() {

		super(6, outputFields);
	}

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry enrty = functionCall.getArguments();
		Tuple tuple = new Tuple();

		String dob = enrty.getString("DOB");
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

		try {
			cal.setTime(dateFormat.parse(dob));
		} catch (ParseException e) {
			e.printStackTrace();
		}

		int month = cal.get(Calendar.MONTH);

		String monthString = new DateFormatSymbols().getMonths()[month];

		tuple.add(enrty.getLong("accno"));
		tuple.add(enrty.getString("name"));
		tuple.add(enrty.getString("DOB"));
		tuple.add(enrty.getInteger("phno"));
		tuple.add(enrty.getString("city"));
		tuple.add(enrty.getString("accntbalance"));
		tuple.add(monthString);

		functionCall.getOutputCollector().add(tuple);

	}

}
