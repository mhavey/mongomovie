package org.jude.bigdata.recroom.movies.etl;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Extract properties for an ETL participant
 * 
 * @author user
 * 
 */
public class ETLProperties {

	Properties props = null;
	Logger logger = Logger.getLogger(ETLProperties.class);

	static ETLProperties theOneInstance = null;

	public static ETLProperties instance() {
		return theOneInstance;
	}

	/**
	 * Default constructor.
	 */
	public ETLProperties() {
		theOneInstance = this;
	}

	/**
	 * Load the given properties file
	 * 
	 * @param propsFile
	 * @throws IOException
	 */
	public void loadProperties(String propsFile) throws ETLException {
		File fProps = new File(propsFile);
		props = new Properties();
		FileReader fr = null;
		try {
			fr = new FileReader(fProps);
			props.load(fr);
		} catch (IOException e) {
			throw new ETLException(ETLConstants.ERR_FILE,
					"Error loading props in file *" + propsFile + "*", e);
		} finally {
			if (fr != null) {
				try {
					fr.close();
				} catch (Throwable t) {
					ETLException.logError(logger, ETLConstants.ERR_FILE,
							"Closing props file reader", t);
				}
			}
		}
	}

	String getProperty(String propName, boolean failIfNotFound)
			throws ETLException {
		String s = props.getProperty(propName);
		if (s != null) {
			s = s.trim();
		}
		if ((s == null || s.equals("")) && failIfNotFound) {
			throw new ETLException(ETLConstants.ERR_FIELD_NOT_FOUND,
					"Required prop not found *" + propName + "*");
		}
		return s;
	}

	/**
	 * Get property with given name. Throw runtime error if not found or not int.
	 * 
	 * @param propName
	 * @return
	 * @throws ETLException
	 */
	public int getInt(String propName) throws ETLException {
		String s = getProperty(propName, true);
		return Integer.parseInt(s);
	}

	/**
	 * Get property with given name. Throw runtime error if not found.
	 * 
	 * @param propName
	 * @return
	 * @throws ETLException
	 */
	public String getString(String propName) throws ETLException {
		return getProperty(propName, true);
	}

	/**
	 * Get property with given name. If not found, return default.
	 * 
	 * @param propName
	 * @param defaultVal
	 * @return
	 * @throws ETLException
	 */
	public String getString(String propName, String defaultVal)
			throws ETLException {
		String s = getProperty(propName, false);
		if (s == null) {
			return defaultVal;
		}
		return s;
	}
}
