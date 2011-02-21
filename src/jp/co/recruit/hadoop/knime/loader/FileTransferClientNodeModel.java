package jp.co.recruit.hadoop.knime.loader;


import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;

import java.io.File;
import java.io.FileInputStream;
// import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.VFS;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;



/**
 *
 * @author Thomas Gabriel, University of Konstanz
 */
final class FileTransferClientNodeModel extends NodeModel {

	DatabaseQueryConnectionSettings conn = null;
	
    private FileTransferClientConfig m_config;

    public Set<String> tables = new HashSet<String>();

	private static final NodeLogger LOGGER =
        NodeLogger.getLogger(FileTransferClientNodeModel.class);

    /**
     * Creates a new database reader.
     */
/*
	// 茶色のDBラインのときはこっちだった
    FileTransferClientNodeModel() {
        super(new PortType[0], new PortType[]{DatabasePortObject.TYPE});
    }
*/
    
    FileTransferClientNodeModel(final int ins, final int outs) {
        super(ins, outs);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    	LOGGER.info("saveSettingsTo");
        if (m_config != null) {
        	m_config.tables = new HashSet<String>(tables);
        	LOGGER.info("tables: " + m_config.tables);
            m_config.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

    	// new FileTransferClientConfig().loadSettingsInModel(settings);
    	
    	// driver,database,user,passwordは予約語ぽい
    	// これらはpackage org.knime.core.node.port.database.DatabaseConnectionSettingsにあり
        if (conn == null) {
        	LOGGER.info("validSettings: conn is created.");
        	conn = new DatabaseQueryConnectionSettings(settings, getCredentialsProvider());
        }else{
        	LOGGER.info("validSettings: conn is OK.");
        }
    	ResultSet result = execHiveQuery("show tables");
    	if(result == null){
        	LOGGER.error("tab_name: (There are no tables)");
    	}else{
			tables.clear();
	    	try {
		        while ( result.next() ) {
			    	// "show tables"の結果表示
		        	LOGGER.info("tab_name: " + result.getString("tab_name"));
		        	tables.add(result.getString("tab_name"));
		        }    	
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }

    protected ResultSet execHiveQuery(String strQuery){
    	ResultSet result = null;
        try {
            if (conn == null) {
                throw new InvalidSettingsException(
                        "No database connection available.");
            }else{
            	LOGGER.info("execHiveQuery: conn may be OK");
            }
            final Connection connObj = conn.createConnection();
            final Statement m_stmt = connObj.createStatement();
            result = m_stmt.executeQuery(strQuery);
        	LOGGER.info("execHiveQuery: " + strQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
    	return result;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {

    	FileTransferClientConfig config = new FileTransferClientConfig();
        config.loadSettingsInModel(settings);
        m_config = config;                
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws CanceledExecutionException,
            IOException {

    	Integer intRet = -2;
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// sftpを行う
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        exec.setProgress("Sending file ...");
    	FileSystemManager fsm = VFS.getManager();
    	FileSystemOptions opts = new FileSystemOptions();
    	
    	// known_hostsのチェックをスキップするよう設定します
    	SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    	builder.setStrictHostKeyChecking(opts, "no");
    	
    	// sftpプロトコルを指定してファイルを取得します
    	// （ホームディレクトリからの相対パス指定はうまく動作しませんでした）
    	String execUrl = "sftp://"
			+ m_config.m_user + ":" + "xxx" + "@"
			+ m_config.m_serverurl + m_config.m_to;    	
    	LOGGER.info("execUrl: " + execUrl);
    	execUrl = "sftp://"
			+ m_config.m_user + ":" + m_config.m_password + "@"
			+ m_config.m_serverurl + m_config.m_to;    	
    	FileObject fileObj = fsm.resolveFile(execUrl, opts);
    	
    	// IOUtilsの使い方
    	// String fileStr = IOUtils.toString(file.getContent().getInputStream(), "UTF-8");
        // IOUtils.write(HELLO_HADOOP_STR, output);

    	InputStream input = new FileInputStream(m_config.m_from);
        OutputStream output = fileObj.getContent().getOutputStream();
        Integer length = copyStream(input,output,2048);
        if(length > 0){
        	intRet++;
        	LOGGER.info("transferedSize: " + length.toString());
        }else{
        	LOGGER.error("transfer was failed.");
        }
        IOUtils.closeQuietly(output);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// JDBC経由で"LOAD DATA LOCAL INPATH './examples/files/kv1.txt' OVERWRITE INTO TABLE pokes"
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
        exec.setProgress("Making Hive load file ...");
        ResultSet result = execHiveQuery(
        		"load data local inpath '" + m_config.m_to + "' overwrite into table " + m_config.table
        );
    	if(result != null){
        	intRet++;
            LOGGER.info("hive load may be OK.");
    	}else{
            LOGGER.error("cannot load data on hive.");
    	}
        
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// 次ノードに引き継ぐテーブルをゼロから作る
    	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  
    	// init spec
        exec.setProgress("Making result table ...");
		BufferedDataContainer container = exec.createDataContainer(makeTableSpec());

		// add arbitrary number of rows to the container
		DataRow firstRow = new DefaultRow(
			new RowKey("first"), // KNIME専用のデータ
			new DataCell[]{new StringCell("result"), new IntCell(intRet)} ); // 実データ
		container.addRowToTable(firstRow);
    	
		/*
    	DataRow secondRow = new DefaultRow(new RowKey("second"), new DataCell[]{
    	    new StringCell("B1"), new DoubleCell(2.0)
    	});
    	container.addRowToTable(secondRow);
    	*/

    	// finally close the container and get the result table.
    	container.close();
    	  
        BufferedDataTable out = container.getTable();
        return new BufferedDataTable[]{out};
    }
    protected DataTableSpec makeTableSpec(){
    	DataTableSpec spec = new DataTableSpec(
    			new DataColumnSpecCreator("A", StringCell.TYPE).createSpec(),
    			new DataColumnSpecCreator("B", IntCell.TYPE).createSpec() );
    	return spec;
    }

    
    protected static int copyStream(InputStream in, OutputStream os,
	    int bufferSize) throws IOException {
	    int len = -1;
	    int length = 0;
	    byte[] b = new byte[bufferSize * 1024];
	    try {
	        while ((len = in.read(b, 0, b.length)) != -1) {
	            os.write(b, 0, len);
	            length += len;
	        }
	        os.flush();
	    } finally {
	        if (in != null) {
	            try {
	                in.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	        if (os != null) {
	            try {
	                os.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	    return length;
	}
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs){
    	LOGGER.info("configure: Model");
    	// ここで返しておけば、exec前でも後続のノードで処理が書ける
        return new DataTableSpec[] {makeTableSpec()};
    }
    
    @Override
    protected void reset() {
    	// for changing jdbc settings.
        if (conn != null) {
        	conn = null;
        }
    	LOGGER.info("reset");
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException {
        // Node has no internal data.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException {
        // Node has no internal data.
    }

    /*    
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
    	LOGGER.error("loadInternals");
        File specFile = null;
        specFile = new File(nodeInternDir, "spec.xml");
        if (!specFile.exists()) {
            IOException ioe = new IOException("Spec file (\""
                    + specFile.getAbsolutePath() + "\") does not exist "
                    + "(node may have been saved by an older version!)");
            throw ioe;
        }
        NodeSettingsRO specSett =
            NodeSettings.loadFromXML(new FileInputStream(specFile));
    }
 	*/
    /**
     * {@inheritDoc}
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
    	LOGGER.error("saveInternals");
        
        NodeSettings specSett = new NodeSettings("spec.xml");
        // m_lastSpec.save(specSett);
        File specFile = new File(nodeInternDir, "spec.xml");
        specSett.saveToXML(new FileOutputStream(specFile));
    }
     */
    
    final DatabaseQueryConnectionSettings createDBQueryConnection(
            final DatabasePortObjectSpec spec, final String newQuery)
    		throws InvalidSettingsException {
    	DatabaseQueryConnectionSettings conn =
    		new DatabaseQueryConnectionSettings(
    			spec.getConnectionModel(), getCredentialsProvider());
        return new DatabaseQueryConnectionSettings(conn, newQuery);
    }
}