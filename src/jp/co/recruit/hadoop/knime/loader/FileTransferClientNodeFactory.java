package jp.co.recruit.hadoop.knime.loader;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "FileTransferClient" Node.
 * test hive.
 *
 * @author Takeshi NAKANO
 */
public class FileTransferClientNodeFactory 
        extends NodeFactory<FileTransferClientNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public FileTransferClientNodeModel createNodeModel() {
        // return new FileTransferClientNodeModel();
    	return new FileTransferClientNodeModel(0,1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<FileTransferClientNodeModel> createNodeView(final int viewIndex,
            final FileTransferClientNodeModel nodeModel) {
        return new FileTransferClientNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
    	return new FileTransferClientDialogPane(true);
    }

}

