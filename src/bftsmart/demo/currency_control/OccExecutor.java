package bftsmart.demo.currency_control;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

class OccState{
    public static int READ_PHASE = 1;
    public static int VALID_AND_WRITE = 2;
}

public class OccExecutor {
    public TransDBWrapper cache;
    public OccLayerDef occ_layer;
    public int start_trans_number = occ_layer.committed_trans_num;
    public Queue<TransDBWrapper> backup_queue = new LinkedList<>();

    public OccExecutor(TransDBWrapper cache, OccLayerDef occ_layer){
        this.cache = cache;
        this.occ_layer = occ_layer;
    }

    public boolean validate() throws IOException{ // call until meet commit operation.
        if (this.cache.get_write_set().size() == 0){ // read-only
            return true;
        }
        int finish_trans_number = occ_layer.committed_trans_num;
        boolean valid = true, succ = true;
        for (int tn = this.start_trans_number + 1; tn <= finish_trans_number; ++tn) {
            TransDBWrapper other_caches = occ_layer.get_cache_by_tn(tn);
            Set<String> read_set  = this.cache.get_read_set();
            Set<String> write_set =  other_caches.get_write_set();
            for (String key : write_set){
                if (read_set.contains(key)) {
                    valid = false;
                    this.backup_queue.add(this.cache);
                    break;

                }
            }
        }
        if (valid){
            succ = replicate_and_write_phase();
            if (succ){
                ++occ_layer.committed_trans_num;
                occ_layer.commit_after_reach_consensus(this.cache);
            }
        }
        if (!valid || !succ){
            backup();
        }
        return succ;
    }


    public boolean replicate_and_write_phase() throws IOException {
//        this.cache.get_wr ite_set()
        List<OperationDef> ops= new ArrayList<>();
        for(String key:this.cache.get_write_set()){
            ops.add(new OperationDef(this.cache.trans_id,OperationType.WRITE, key, this.cache.copies.get(key)));
        }
        byte reply[] = occ_layer.KVProxy.invokeOrdered(MessageDef.assemble_ops_into_stream(ops).toByteArray());
        return reply != null;
    }

    private void backup(){
        // At this time, simply use abort to implement backup.
        while(this.backup_queue.peek() != null){
            TransDBWrapper _cache = this.backup_queue.poll();
            _cache.abort();
        }
        System.out.println("Backup succeed.");
    }
}
