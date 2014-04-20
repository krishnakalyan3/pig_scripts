package UDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class Regex extends EvalFunc<String> {
	static HashMap<String, String> map = new HashMap<String, String>();

	public List<String> getCacheFiles() {
		Path lookup_file = new Path(
				"hdfs://localhost.localdomain:8020/user/cloudera/top");
		List<String> list = new ArrayList<String>(1);
		list.add(lookup_file + "#id_lookup");
		return list;
	}

	public void VectorizeData() throws IOException {
		FileReader fr = new FileReader("./id_lookup");
		BufferedReader brd = new BufferedReader(fr);
		String line;
		while ((line = brd.readLine()) != null) {
			String str[] = line.split("#");
			map.put(str[0], str[1]);
		}
		fr.close();
	}

	private String Regex(String tweet) throws ExecException {
		// TODO Auto-generated method stub

		for (Entry<String, String> entry : map.entrySet()) {
			Pattern r = Pattern.compile(map.get(entry.getKey()));
			Matcher m = r.matcher(tweet);
			if (m.find() == true) {
				return entry.getValue();
			}
		}

		return null;
	}

	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1 || input.get(0) == null)
			return null;
		try {
			
			VectorizeData();
			String str = (String) input.get(0);

			return Regex(str);

		} catch (Exception e) {
			throw WrappedIOException.wrap(
					"Caught exception processing input row ", e);
		}
	}
}