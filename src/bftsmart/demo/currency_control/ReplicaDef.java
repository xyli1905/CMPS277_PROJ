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

    public MapOfMaps localDB;
    public String tableName = "Master Lee";
    public OccLayerDef occLayer;

    //ServiceReplica replica = null;
    //private ReplicaContext replicaContext;

    //The constructor passes the id of the server to the super class

    public ReplicaDef(int id){
        initDB();
        occLayer = new OccLayerDef(id, this);
        new ServiceReplica(id, this, this);
    }

    public void initDB(){
        this.localDB = new MapOfMaps();
        localDB.addTable(tableName, new HashMap<String, byte[]>());
        localDB.addData(tableName, "a", "1".getBytes());
        localDB.addData(tableName, "b", "2".getBytes());
        localDB.addData(tableName, "c", "3".getBytes());
        localDB.addData(tableName, "d", "4".getBytes());
    }

    public static void main(String[] args){
        if(args.length < 1) {
            System.out.println("Use: java BFTMapServer <processId>");
            System.exit(-1);
        }
        new ReplicaDef(Integer.parseInt(args[0]));
    }

    public String readFromReplicaLocalDB(String k){
//        System.out.println("In file ReplicaDef, In function readFromReplicaLocalDB, key is : " + k);
        byte[] reply = this.localDB.getEntry(tableName, k);
        if (reply == null) return ""; //TODO
        return reply.toString();
    }

    public void writeToReplicaLocalDB(String k, String v){
//        System.out.println("In file ReplicaDef, In function writeToReplicaLocalDB, key is : " + k + " value is: " + v);
        this.localDB.addData(tableName,k, v.getBytes());
    }

    public Map<String, String> getDBCopy(){
        Map<String, String> map = new HashMap<>();
        Map<String, byte[]> localMap = this.localDB.getTable(tableName);
        for(String k:map.keySet()){
            map.put(k, localMap.get(k).toString());
        }
        return map;
    }


    public byte[] recieveMessage(OperationDef op) {
        // can't recieve msg here
        //System.out.println(KVProxy);

        if (occLayer.KVProxy == null) return null;

        byte[] reply = null;
//        System.out.println("###################################");
//        System.out.println("get op:" + op.toString());
        if (op.type == OperationType.READ) {
            String replyStr = occLayer.create_or_update_executor_by_read_op(op);
//            System.out.println("###################################");
//            System.out.println("READ: "+ replyStr);
            if (replyStr == null) return new byte[]{};
            return replyStr.getBytes();
        } else if (op.type == OperationType.WRITE) {
            occLayer.create_or_update_executor_by_write_op(op);
        } else if (op.type == OperationType.ABORT) {
            occLayer.get_executor_by_id(op.trans_id).cache.abort();
        } else if (op.type == OperationType.COMMIT) {
            boolean succ = occLayer.create_or_update_executor_by_commit_op(op);
            if (!succ) {
                occLayer.get_executor_by_id(op.trans_id).cache.abort();
            } else {
                reply = new byte[]{1};
            }

        }
        return reply;
    }


    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
//        System.out.println("+++++++++++++ In file ReplicaDef, In function appExecuteOrdered +++++++++++++");
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
                    occLayer.get_executor_by_id(op.trans_id).cache.abort();
                }else if(op.type == OperationType.COMMIT){
                    boolean succ = occLayer.create_or_update_executor_by_commit_op(op);
                    if(!succ){
                        occLayer.get_executor_by_id(op.trans_id).cache.abort();
                    }
                }
            }else{
                List<OperationDef> ops = MessageDef.parse_ops_from_stream(in);
                for(OperationDef op:ops){
                    this.writeToReplicaLocalDB(op.key, op.val);
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
//        System.out.println("+++++++++++++ In file ReplicaDef, In function appExecuteOrdered +++++++++++++");
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
                    occLayer.get_executor_by_id(op.trans_id).cache.abort();
                }else if(op.type == OperationType.COMMIT){
                    boolean succ = occLayer.create_or_update_executor_by_commit_op(op);
                    if(!succ){
                        occLayer.get_executor_by_id(op.trans_id).cache.abort();
                    }
                }
            }else{
                List<OperationDef> ops = MessageDef.parse_ops_from_stream(in);
                for(OperationDef op:ops){
                    this.writeToReplicaLocalDB(op.key, op.val);
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
            out.writeObject(localDB);
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
            localDB = (MapOfMaps) in.readObject();
            in.close();
            bis.close();

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //    @Override
//    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
//        System.out.println("+++++++++++++ In file ReplicaDef, In function appExecuteOrdered +++++++++++++");
//        YCSBMessage request = YCSBMessage.getObject(command);
//        switch (request.getType()){
//            case WRITE:
//                occLayer.create_or_update_executor_by_write_op(request);
//                break;
//            case COMMIT:
//                occLayer.create_or_update_executor_by_commit_op(request);
//                break;
//        }
//        return new byte[0];
//    }
//
//    @Override
//    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
//        System.out.println("+++++++++++++ In file ReplicaDef, In function appExecuteOrdered +++++++++++++");
//        YCSBMessage request = YCSBMessage.getObject(command);
//        if(request == null){
//            System.out.println("YCSBMessage is NULL");
//        }else{
//            System.out.println("YCSBMessage in Server:  " + request.toString());
//        }
//        String reply = "";
//        if (request.getType() == YCSBMessage.Type.READ){
//            reply = occLayer.create_or_update_executor_by_read_op(request);
//        }
//        return reply.getBytes();
//    }

}