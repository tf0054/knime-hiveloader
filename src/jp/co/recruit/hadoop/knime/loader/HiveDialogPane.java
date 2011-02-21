package jp.co.recruit.hadoop.knime.loader;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseDriverLoader;
import org.knime.core.util.KnimeEncryption;
import javax.swing.JFileChooser;
/**
 *
 * @author Takeshi NAKANO
 */

final class HiveDialogPane extends JPanel {

    private static final NodeLogger LOGGER =
        NodeLogger.getLogger(HiveDialogPane.class);

    private final JComboBox m_driver = new JComboBox();

    private final JComboBox m_db = new JComboBox();
    
    private final JComboBox m_table = new JComboBox();

    static JFileChooser fc = new JFileChooser( "." );
    
    private final JTextField m_serverurl = new JTextField("");
    private final JTextField m_from = new JTextField("");
    private final JTextField m_to = new JTextField("");
    
    private final JTextField m_user = new JTextField("");
    private final JPasswordField m_pass = new JPasswordField();

    private boolean m_passwordChanged = false;

//    private final JCheckBox m_credCheckBox = new JCheckBox();
//    private final JComboBox m_credBox = new JComboBox();

    /** Default font used for all components within the database dialogs. */
    static final Font FONT = new Font("Monospaced", Font.PLAIN, 12);


    /**
     * Creates new dialog.
     */
    HiveDialogPane() {
        super(new GridLayout(0, 1));
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        m_driver.setEditable(false);
        m_driver.setFont(FONT);
        m_driver.setPreferredSize(new Dimension(400, 20));
        m_driver.setMaximumSize(new Dimension(400, 20));
        
        JPanel driverPanel = new JPanel(new BorderLayout());
        driverPanel.setBorder(BorderFactory
                .createTitledBorder(" Database driver "));
        driverPanel.add(m_driver, BorderLayout.CENTER);
        driverPanel.add(new JLabel(" (Additional Database Drivers can be loaded"
                + " in the KNIME preference page.) "), BorderLayout.SOUTH);
        super.add(driverPanel);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel dbPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        dbPanel.setBorder(BorderFactory.createTitledBorder(
                " Database URL "));
        m_db.setFont(FONT);
        m_db.setPreferredSize(new Dimension(400, 20));
        m_db.setMaximumSize(new Dimension(400, 20));
        m_db.setEditable(true);
        dbPanel.add(m_db);
        super.add(dbPanel);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel tablePanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        tablePanel.setBorder(BorderFactory.createTitledBorder(
                " Tables "));
        m_table.setFont(FONT);
        m_table.setPreferredSize(new Dimension(400, 20));
        m_table.setMaximumSize(new Dimension(400, 20));
        m_table.setEditable(true);
        tablePanel.add(m_table);
        super.add(tablePanel);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel serverPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        serverPanel.setBorder(BorderFactory.createTitledBorder(
                " Server URL "));
        m_serverurl.setFont(FONT);
        m_serverurl.setPreferredSize(new Dimension(400, 20));
        m_serverurl.setMaximumSize(new Dimension(400, 20));
        m_serverurl.setEditable(true);

        serverPanel.add(m_serverurl);
        super.add(serverPanel);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel fromPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        fromPanel.setBorder(BorderFactory.createTitledBorder(" From "));
        m_from.setPreferredSize(new Dimension(400, 20));
        m_from.setFont(FONT);
        // ファイル選択画面
		JButton button = new JButton("Browse");
		button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int result = fc.showOpenDialog( m_from );
				if(result == JFileChooser.APPROVE_OPTION) {
					m_from.setText( fc.getSelectedFile().getPath() );
					if(m_to.getText().length() == 0){
						m_to.setText( "/tmp/" + fc.getSelectedFile().getName() );
					}
				}
				if(result == JFileChooser.CANCEL_OPTION) {
					m_from.setText( "canceled" );
				}
				if(result == JFileChooser.ERROR_OPTION) {
					m_from.setText( "canceled or error" );
				}
			}
		});
		// パネルに貼り付け
        fromPanel.add(m_from);
        fromPanel.add(button);
        super.add(fromPanel);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel toPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        toPanel.setBorder(BorderFactory.createTitledBorder(" To "));
        m_to.setPreferredSize(new Dimension(400, 20));
        m_to.setFont(FONT);
        toPanel.add(m_to);
        super.add(toPanel);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel userPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        userPanel.setBorder(BorderFactory.createTitledBorder(" User name "));
        m_user.setPreferredSize(new Dimension(400, 20));
        m_user.setFont(FONT);
        userPanel.add(m_user);
        super.add(userPanel);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        JPanel passPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        passPanel.setBorder(BorderFactory.createTitledBorder(" Password "));
        m_pass.setPreferredSize(new Dimension(400, 20));
        m_pass.setFont(FONT);
        m_pass.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void changedUpdate(final DocumentEvent e) {
                m_passwordChanged = true;
            }
            @Override
            public void insertUpdate(final DocumentEvent e) {
                m_passwordChanged = true;
            }
            @Override
            public void removeUpdate(final DocumentEvent e) {
                m_passwordChanged = true;
            }
        });
        m_pass.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(final FocusEvent fe) {
                if (!m_passwordChanged) {
                    m_pass.setText("");
                }
            }
        });
        passPanel.add(m_pass);
        super.add(passPanel);
    }
    
    private void updateDriver() {
        m_driver.removeAllItems();
        Set<String> driverNames = new HashSet<String>(
                DatabaseDriverLoader.getLoadedDriver());
        for (String driverName
                : DatabaseConnectionSettings.DRIVER_ORDER.getHistory()) {
            if (driverNames.contains(driverName)) {
                m_driver.addItem(driverName);
                driverNames.remove(driverName);
            }
        }
        for (String driverName : driverNames) {
            m_driver.addItem(driverName);
        }
    }

    private void updateTable(Set<String> tables) {
    	m_table.removeAllItems();
    	if(tables != null){
	        for (String tableName : tables) {
            	m_table.addItem(tableName);
	        }
	    }
    }
    
    /**
     * Load settings.
     * @param settings to load
     * @param specs input spec
     * @param creds credentials
     */
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) {
    	
    	// 都度newしてsettingsから戻しているので、tablesは消えちゃう。。。。    	
    	FileTransferClientConfig config = new FileTransferClientConfig();
        config.loadSettingsInDialog(settings);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        m_driver.removeAllItems();
        // update list of registered driver
        updateDriver();
        String select = settings.getString("driver", m_driver.getSelectedItem().toString());
        if(select == null){
        	m_driver.setSelectedItem(config.driver);
        }else{
            m_driver.setSelectedItem(select);        	
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        m_db.removeAllItems();
        for (String databaseURL
                : DatabaseConnectionSettings.DATABASE_URLS.getHistory()) {
        	m_db.addItem(databaseURL);
        }
        String dbName = settings.getString("database", m_db.getEditor().getItem().toString());
        if (dbName == null) {
        	m_db.setSelectedItem(config.database);
        } else {
        	m_db.setSelectedItem(dbName);
        }
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        m_table.removeAllItems();
        if(config.tables != null){
	    	LOGGER.error("dialog-tables: "+config.tables.size());
	        updateTable(config.tables);
        }
        String tableName = settings.getString("table", m_table.getEditor().getItem().toString());
        if (tableName == null) {
        	m_table.setSelectedItem(config.table);
        } else {
        	m_table.setSelectedItem(tableName);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        String strServer = settings.getString("serverurl", null);
        m_serverurl.setText(strServer == null ? "" : config.m_serverurl);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        String strFrom = settings.getString("from", null);
        m_from.setText(strFrom == null ? "" : config.m_from);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        String strTo = settings.getString("to", null);
        m_to.setText(strTo == null ? "" : config.m_to);
        /*
        LOGGER.error("loadSettingsFrom from/to: " 
        		+ settings.getString("from", null)
        		+ "/" 
        		+ settings.getString("to", null));
        LOGGER.error("loadSettingsFrom id/pw: " 
        		+ settings.getString("suser", null)
        		+ "/" 
        		+ settings.getString("spassword", null));
		*/
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        String user = settings.getString("suser", null);
        m_user.setText(user == null ? "" : config.m_user);
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        String password = settings.getString("spassword", null);
        m_pass.setText(password == null ? "" : config.m_password);
        m_passwordChanged = false;        
    }

    /**
     * Save settings.
     * @param settings to save into
     */
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    	FileTransferClientConfig config = new FileTransferClientConfig();
    	LOGGER.info("saveSettingsTo: DBDialogPane");

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("driver", m_driver.getSelectedItem().toString());
        config.driver = m_driver.getSelectedItem().toString();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("database", m_db.getEditor().getItem().toString());
        config.database = m_db.getEditor().getItem().toString();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("table", m_table.getEditor().getItem().toString());
        config.table = m_table.getEditor().getItem().toString();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("serverurl", m_serverurl.getText().trim());
    	config.m_serverurl = m_serverurl.getText().trim();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    	settings.addString("from", m_from.getText().trim());
    	config.m_from = m_from.getText().trim();
    	
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("to", m_to.getText().trim());
    	config.m_to = m_to.getText().trim();

    	//LOGGER.error("saveSettingsTo from/to: "+ m_from.getText().trim()+"/"+m_to.getText().trim());
        
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("suser", m_user.getText().trim());
    	config.m_user = m_user.getText().trim();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("spassword", m_pass.getText());
        config.m_password = new String(m_pass.getText());

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        settings.addString("statement", "show tables");
    	config.statement = "show tables";
        settings.addString("user", "u");
    	config.user = "u";
        settings.addString("password", "p");
    	config.password = "p";
  }
}

