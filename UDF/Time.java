package UDF;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Time extends EvalFunc<String> {

	public String exec(Tuple input) throws IOException {

		if ((input == null) || (input.size() == 0))
			return null;
		try {
			String time = (String) input.get(0);
			SimpleDateFormat parserSDF = new SimpleDateFormat(
					"EEE MMM d HH:mm:ss zzz yyyy");
			
			Date dateStr = parserSDF.parse(time);
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			String formattedDate = formatter.format(dateStr);

			return formattedDate;
		} catch (ParseException e) {

			return null;
		}

	}
}
