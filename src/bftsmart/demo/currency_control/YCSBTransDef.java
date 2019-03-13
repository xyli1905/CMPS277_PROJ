package bftsmart.demo.currency_control;

import bftsmart.tom.ServiceProxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class YCSBTransDef {
    public String trans_id = "";
    public ByteArrayOutputStream output;
    public ServiceProxy proxy;

    public YCSBTransDef(ServiceProxy proxy, String trans_id){
        this.proxy = proxy;
        this.trans_id = trans_id;
    }

    public byte[] read(String table, String key, Set<String> fields, HashMap<String, byte[]> resultes){
        YCSBMessage request = YCSBMessage.newReadRequest(table, key, fields, resultes, trans_id);
        System.out.println("In file YCSBTransDef, In function read, request is: " + request.toString());
        byte[] reply = proxy.invokeUnordered(request.getBytes()); //
        System.out.println("In file YCSBTransDef, In function read, reply is: " + reply.toString());
        return reply;
    }

    public void write(String table, String key, HashMap<String, byte[]> values){
        YCSBMessage request = YCSBMessage.newWriteRequest(table, key, values, trans_id);
        System.out.println("In file YCSBTransDef, In function write, request is: " + request.toString());
        byte[] reply = proxy.invokeOrdered(request.getBytes());
        System.out.println("In file YCSBTransDef, In function write, reply is: " + reply.toString());
    }

    public byte[] commit(){
        HashMap<String, byte[]> results = new HashMap<>();
        YCSBMessage request = YCSBMessage.newCommitRequest("", "", null, results, trans_id);
        byte[] reply = proxy.invokeOrdered(request.getBytes());
        System.out.println("In file YCSBTransDef, In function commit, reply is: " + reply.toString());
        return reply;
    }
}
