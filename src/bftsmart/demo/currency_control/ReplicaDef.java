package bftsmart.demo.currency_control;

import bftsmart.demo.bftmap.BFTMapRequestType;
import bftsmart.demo.bftmap.BFTMapServer;
import bftsmart.demo.bftmap.MapOfMaps;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicaDef extends DefaultSingleRecoverable {

    public MapOfMaps DB;
    public String tableName = "Master Lee";
    public OccLayerDef occLayer;

    //ServiceReplica replica = null;
    //private ReplicaContext replicaContext;

    //The constructor passes the id of the server to the super class
    public ReplicaDef(int id) {
        DB = new MapOfMaps();
        DB.addTable(tableName, new HashMap<>());
        occLayer = new OccLayerDef(id, this);
        new ServiceReplica(id, this, this);
    }

    public static void main(String[] args){
        if(args.length < 1) {
            System.out.println("Use: java BFTMapServer <processId>");
            System.exit(-1);
        }
        new ReplicaDef(Integer.parseInt(args[0]));
    }

    public String readFromUniqueDB(String k){
        byte[] reply = this.DB.getEntry(tableName, k);
        return reply.toString();
    }

    public void writeToUniqueDB(String k, String v){
        this.DB.addData(tableName,k, v.getBytes());
    }

    public Map<String, String> getDBCopy(){
        Map<String, String> map = new HashMap<>();
        Map<String, byte[]> localMap = this.DB.getTable(tableName);
        for(String k:map.keySet()){
            map.put(k,localMap.get(k).toString());
        }

        return map;
    }

    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            if (cmd == MessageType.SINGLE_OP){
                OperationDef op = MessageDef.parse_op_from_stream(in);
                if(op.type == OperationType.READ){
                    String replyStr = occLayer.create_or_update_executor_by_read_op(op);
                    return replyStr.getBytes();
                }else if(op.type == OperationType.WRITE){
                    occLayer.create_or_update_executor_by_write_op(op);
                }else if(op.type == OperationType.ABORT){
                    occLayer.get_cache_by_tn(op.trans_id).abort();
                }else if(op.type == OperationType.COMMIT){
                    boolean succ = occLayer.create_or_update_executor_by_commit_op(op);
                    if(!succ){
                        occLayer.get_cache_by_tn(op.trans_id).abort();
                    }
                }
            }else{
                List<OperationDef> ops = MessageDef.parse_ops_from_stream(in);
                for(OperationDef op:ops){
                    this.writeToUniqueDB(op.key, op.val);
                }
            }
            return reply;
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            if (cmd == MessageType.SINGLE_OP){
                OperationDef op = MessageDef.parse_op_from_stream(in);
                if(op.type == OperationType.READ){
                    String replyStr = occLayer.create_or_update_executor_by_read_op(op);
                    return replyStr.getBytes();
                }else if(op.type == OperationType.WRITE){
                    occLayer.create_or_update_executor_by_write_op(op);
                }else if(op.type == OperationType.ABORT){
                    occLayer.get_cache_by_tn(op.trans_id).abort();
                }else if(op.type == OperationType.COMMIT){
                    boolean succ = occLayer.create_or_update_executor_by_commit_op(op);
                    if(!succ){
                        occLayer.get_cache_by_tn(op.trans_id).abort();
                    }
                }
            }else{
                List<OperationDef> ops = MessageDef.parse_ops_from_stream(in);
                for(OperationDef op:ops){
                    this.writeToUniqueDB(op.key, op.val);
                }
            }
            return reply;
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    @Override
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(DB);
            out.flush();
            bos.flush();
            out.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
            return new byte[0];
        }
    }

    @Override
    public void installSnapshot(byte[] state) {
        try {

            // serialize to byte array and return
            ByteArrayInputStream bis = new ByteArrayInputStream(state);
            ObjectInput in = new ObjectInputStream(bis);
            DB = (MapOfMaps) in.readObject();
            in.close();
            bis.close();

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


}