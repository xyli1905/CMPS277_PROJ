package bftsmart.demo.currency_control;

class OperationType {
    public static int READ   = 1;
    public static int WRITE  = 2;
    public static int COMMIT = 3;
    public static int ABORT  = 4;
}

public class OperationDef{
    public OperationDef(String trans_id, int type, String key, String val){
        this.type = type;
        this.key = key;
        this.val = val;
        this.trans_id = trans_id;
    }
    public int type;
    public String trans_id, key, val;
}


