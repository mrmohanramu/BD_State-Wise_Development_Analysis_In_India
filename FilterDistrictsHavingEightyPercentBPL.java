package project2UDF;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FilterDistrictsHavingEightyPercentBPL extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		try {
			if (input == null || input.size() == 0) {
				return false;
			}

			Object valueTuple = input.get(0);
			if (valueTuple instanceof Tuple) {
				Object value1 = ((Tuple) valueTuple).get(0);
				Object value2 = ((Tuple) valueTuple).get(1);

				long objective_value = Long.valueOf((String) value1);
				long performance_value = Long.valueOf((String) value2);

				if (performance_value > objective_value * 80 / 100) {
					return true;
				}
			}
			
		} catch (ExecException ee) {
			throw ee;
		}
		return false;
	}

}
