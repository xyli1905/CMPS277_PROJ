package bftsmart.demo.currency_control;
import java.util.ArrayList;
import java.util.List;

public class TransactionDef {
    public int trans_id;
    public List<OperationDef> ops = new ArrayList<>();

    public TransactionDef(OperationDef op){
        this.trans_id = op.trans_id;
        this.ops.add(op);
    }

    public TransactionDef(List<OperationDef> ops){
        this.trans_id = ops.get(0).trans_id;
        this.ops = ops;
    }

    public void add_transactions(OperationDef op){
        this.ops.add(op);
    }

    public void add_transactions(List<OperationDef> ops){
        for (OperationDef op : ops){
            this.ops.add(op);
        }
    }
}
