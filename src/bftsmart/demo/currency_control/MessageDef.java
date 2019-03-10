package bftsmart.demo.currency_control;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class MessageType{
    public static int SINGLE_OP = 1;
    public static int MULTI_OPS = 2;
    public static int WRITE_SET = 3;
}

public class MessageDef {
    public static ByteArrayOutputStream assemble_keys_into_stream(Set<String> keyset) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(MessageType.WRITE_SET);
        dos.writeInt(keyset.size());
        Iterator<String> it = keyset.iterator();
        while (it.hasNext())
            dos.writeUTF(it.next());
        return out;
    }

    public static List<String> parse_keys_from_stream(ByteArrayInputStream in) throws IOException {
        DataInputStream input = new DataInputStream(in);
        int size = input.readInt();
        List<String> keys = new ArrayList<>();
        for(int i = 0; i < size; i++)
            keys.add(input.readUTF());
        return keys;
    }

    public static OperationDef parse_op_from_stream(ByteArrayInputStream in) throws IOException {
        int type = new DataInputStream(in).readInt();
        int id = new DataInputStream(in).readInt();
        String key = new DataInputStream(in).readUTF();
        String val = new DataInputStream(in).readUTF();
        return new OperationDef(id, type, key, val);
    }

    public static ByteArrayOutputStream assemble_op_into_stream(OperationDef op) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(MessageType.SINGLE_OP);
        dos.writeInt(op.trans_id);
        dos.writeInt(op.type);
        dos.writeUTF(op.key);
        dos.writeUTF(op.val);
        return out;
    }

    public static List<OperationDef> parse_ops_from_stream(ByteArrayInputStream in) throws IOException {
        List<OperationDef> ops = new ArrayList<>();
        DataInputStream input = new DataInputStream(in);
        int size = new DataInputStream(in).readInt();
        for (int i = 0; i < size; ++i){
            int type = new DataInputStream(in).readInt();
            int id = new DataInputStream(in).readInt();
            String key = new DataInputStream(in).readUTF();
            String val = new DataInputStream(in).readUTF();
            ops.add(new OperationDef(id, type, key, val));
        }
        return ops;
    }

    public static ByteArrayOutputStream assemble_ops_into_stream(List<OperationDef> ops)throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(MessageType.MULTI_OPS);
        dos.writeInt(ops.size());
        for (OperationDef op : ops){
            dos.writeInt(op.trans_id);
            dos.writeInt(op.type);
            dos.writeUTF(op.key);
            dos.writeUTF(op.val);
        }
        return out;
    }
}
