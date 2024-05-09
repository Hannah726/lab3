package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transId;
    private DbIterator child;
    private final int tableId;
    
    private static final TupleDesc SCHEMA = new TupleDesc(new Type[] { Type.INT_TYPE });

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (t == null || child == null) {
    		throw new IllegalArgumentException();
    	}
    	
        transId = t;
        this.child = child;
        tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return SCHEMA;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        throw new DbException("Rewind is not supported for INSERT.");
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (child == null) {
    		return null;
    	}
    	
    	child.open();
    	BufferPool bp = Database.getBufferPool();
    	int inserted = 0;
    	while (child.hasNext()) {
    		Tuple t = child.next();
    		try {
				bp.insertTuple(transId, tableId, t);
			} catch (IOException e) {
				throw new DbException("IO operation failed during INSERT.");
			}
    		inserted++;
    	}
    	child.close();
    	child = null;
    	
    	return new Tuple(SCHEMA, new Field[] { new IntField(inserted) });
        
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (children.length != 1) {
        	throw new IllegalArgumentException("Expected one new child.");
        }
        child = children[0];
    }
}
