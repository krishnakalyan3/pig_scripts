package UDF;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class Clean extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1 || input.get(0) == null)
			return null;
		try {
			// Input
			String str = (String) input.get(0);

			return extractQuery(str);

		} catch (Exception e) {
			throw WrappedIOException.wrap(
					"Caught exception processing input row ", e);
		}
	}

	private String extractQuery(String str) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		if (!str.isEmpty()) {
			str = str.trim().replaceAll("[^a-zA-Z0-9\\s]", "");
			str = str.replaceAll("[\\p{Punct}]+", "");
			str = str.trim().replaceAll("[\\s]+", " ");
		}

		return str;
	}
}