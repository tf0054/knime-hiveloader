package jp.co.recruit.hadoop.knime.loader;

import java.util.HashSet;
import java.util.Set;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.database.DatabaseDriverLoader;

final class FileTransferClientConfig {

    public String m_from;
    public String m_to;
    public String m_user;
    public String m_password;
    public String m_serverurl;
    public String driver;
    public String database;
    public String user = "x";
    public String password = "x";
    public String statement = "show tables";
    
    public String table = "";
    public Set<String> tables = null;

    public void loadSettingsInDialog(final NodeSettingsRO settings) {
    	m_from = settings.getString("from", "from");
    	m_to = settings.getString("to", "to");
    	m_user = settings.getString("suser", "suser");
    	m_password = settings.getString("spassword", "spassword");
    	m_serverurl = settings.getString("serverurl", "serverurl");
        // for database
    	user = settings.getString("user", "user");
    	password = settings.getString("password", "password");
    	driver = settings.getString("driver", "driver");
    	database = settings.getString("database", "database");
    	statement = settings.getString("statement", "show tables");
    	table = settings.getString("table", "table");
    	try {
			tables = new HashSet<String>(java.util.Arrays.asList(settings.getStringArray("tables")));
		} catch (InvalidSettingsException e) {
			e.printStackTrace();
		};
    }
    
    public void loadSettingsInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
    	m_from = settings.getString("from");
    	m_to = settings.getString("to");
    	m_user = settings.getString("suser");
    	m_password = settings.getString("spassword");
    	m_serverurl = settings.getString("serverurl");
        // for database
    	user = settings.getString("user");
    	password = settings.getString("password");
    	driver = settings.getString("driver");
    	database = settings.getString("database");
    	statement = settings.getString("statement");
    	// for selecting-table
    	table = settings.getString("table");
    	try {
			tables = new HashSet<String>(java.util.Arrays.asList(settings.getStringArray("tables")));
		} catch (InvalidSettingsException e) {
			e.printStackTrace();
		};
    }
    
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString("from", m_from);
        settings.addString("to", m_to);
        settings.addString("suser", m_user);
        settings.addString("spassword", m_password);
        settings.addString("serverurl", m_serverurl);
        // for database
        settings.addString("user", user);
        settings.addString("password", password);
        settings.addString("driver", driver);
        settings.addString("database", database);
        settings.addString("statement", statement);
        
        settings.addString("table", table);
        settings.addStringArray("tables", tables.toArray(new String[0]));
    }
}
