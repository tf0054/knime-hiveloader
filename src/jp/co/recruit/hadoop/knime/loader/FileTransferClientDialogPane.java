package jp.co.recruit.hadoop.knime.loader;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Collection;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JEditorPane;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;

import org.knime.base.node.io.database.DBVariableSupportNodeModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;

/**
 *
 * @author Thomas Gabriel, University of Konstanz
 */
class FileTransferClientDialogPane extends NodeDialogPane {

    private final boolean m_hasLoginPane;
    private final HiveDialogPane m_loginPane = new HiveDialogPane();

    private final JEditorPane m_statmnt = new JEditorPane("text", "");

    private final DefaultListModel m_listModelVars;
    private final JList m_listVars;

    /**
     * Creates new dialog.
     * @param hasLoginPane true, if a login pane is visible, otherwise false
     */
    FileTransferClientDialogPane(final boolean hasLoginPane) {
        super();
                
        JPanel allPanel = new JPanel(new BorderLayout());

        m_hasLoginPane = hasLoginPane;
        if (hasLoginPane) {
            allPanel.add(m_loginPane, BorderLayout.NORTH);
        }

        // init variable list
        m_listModelVars = new DefaultListModel();
        m_listVars = new JList(m_listModelVars);

        super.addTab("Settings", allPanel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
            m_loginPane.loadSettingsFrom(settings, specs);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        //if (m_hasLoginPane) {
            m_loginPane.saveSettingsTo(settings);
        //}
        /*
        settings.addString(DatabaseConnectionSettings.CFG_STATEMENT,
                m_statmnt.getText().trim());
        */
    }
}
