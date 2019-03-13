package bftsmart.demo.currency_control;

import bftsmart.demo.bftmap.BFTMap;
import bftsmart.demo.bftmap.BFTMapRequestType;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import static bftsmart.demo.currency_control.OperationType.READ;
import static bftsmart.demo.currency_control.OperationType.WRITE;
import static bftsmart.demo.currency_control.OperationType.COMMIT;

/**
 * Created by lijin on 3/10/19.
 */

class JTestHelper{
    public Set<Integer> processTranscationSet = new HashSet<>();
    public Set<String > keyset = new HashSet<>();
    public Set<String > valueset = new HashSet<>();
    Random r = new Random();
    JTestHelper(){
        for (int i = 0 ; i < 100; i++)
            keyset.add(i + "");

        for (int i = 0 ; i < 100; i++)
            valueset.add(i + "Value");
    }

    public String getRandomKey(){
        int i = r.nextInt(99);
        for (String s:keyset){
            if (i == 0) return s;
            i--;
        }
        return "1";
    }

    public String getRandomValue(){
        int i = r.nextInt(99);
        for (String s:valueset){
            if (i == 0) return s;
            i--;
        }
        return "1";
    }

    public int getRandomTid(){
        int i = r.nextInt(processTranscationSet.size());
        for (Integer s:processTranscationSet){
            if (i == 0) return s;
            i--;
        }
        return 0;
    }

    public OperationDef createReadOperation(int tid, String key){
        return new OperationDef(tid, READ, key, "");
    }

    public OperationDef createWriteOperation(int tid, String  key, String  value){
        return new OperationDef(tid, WRITE, key, value);
    }

    public OperationDef createCommitOperation(int tid){
        return new OperationDef(tid, OperationType.COMMIT, "", "");
    }

    public OperationDef generateOperation(){
        int tid = r.nextInt(10000);
        int oprType = r.nextInt(5);
        if (processTranscationSet.size() > 0 && oprType == 3){
            tid = getRandomTid();
            processTranscationSet.remove(tid);
            return createCommitOperation(tid);
        }else{
            processTranscationSet.add(tid);
            if (r.nextBoolean()){
                return createReadOperation(tid, getRandomKey());
            }else{
                return createWriteOperation(tid, getRandomKey(), getRandomValue());
            }
        }
    }
}

public class UserTest {
    private static int read_counter = 0;
    private static int commit_counter = 0;
    private static int write_counter = 0;
    private static long lantency = 0;
    private static long previous = 0;
    public static long getCurrentTime(){
        Date d = new Date();
        return d.getTime();
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 2) {
            System.out.println("Usage: java BFTMapInteractiveClient <process id>");
            System.exit(-1);
        }

        ReplicaDef replica = new ReplicaDef(Integer.parseInt(args[1]));
        Console console = System.console();
        Scanner sc = new Scanner(System.in);
        JTestHelper helper = new JTestHelper();

        previous = getCurrentTime();

        while (true){
            if (getCurrentTime() - previous >= 100) {
                System.out.println("----------------------------------------------------");
                System.out.println();
                System.out.println("handle " + read_counter +" read operation");
                System.out.println("handle " + write_counter +" write operation");
                System.out.println("handle " + commit_counter +" commit operation");
                System.out.println("with total lantency " + lantency + " ms");
                System.out.println();
                System.out.println("----------------------------------------------------");
                write_counter = 0;
                read_counter = 0;
                commit_counter = 0;
                lantency = 0;
                previous = getCurrentTime();
            }
            OperationDef op = helper.generateOperation();
            byte[] reply;
            long b = getCurrentTime();
            if (op.type == READ) {
                reply = replica.recieveMessage(op);
                read_counter++;
                lantency = lantency + getCurrentTime() - b;
            }else if (op.type == WRITE) {
                reply = replica.recieveMessage(op);
                write_counter++;
                lantency = lantency + getCurrentTime() - b;
            }else if (op.type == COMMIT) {
                replica.recieveMessage(op);
                commit_counter++;
                lantency = lantency + getCurrentTime() - b;
            }
        }
    }
}
