package bftsmart.demo.currency_control;
import bftsmart.tom.ServiceProxy;

import java.io.IOException;
import java.util.HashMap;
import java.util.*;

public class OccLayerDef {
    public ServiceProxy KVProxy;
    public ReplicaDef replica;
    public  Map<Integer, TransDBWrapper> wrapper_trans_map = new HashMap<>(); // key: tnc
    public  Map<String, OccExecutor> executor_trans_map = new HashMap<>(); // key: id
    public  int committed_trans_num = 0;

    public OccLayerDef(int id, ReplicaDef replica){
        KVProxy = new ServiceProxy(id, "config");
        this.replica = replica;
    }

    public String create_or_update_executor_by_read_op(YCSBMessage op){
        if (!this.executor_trans_map.containsKey(op.trans_id)){ // doesn't exist
            TransDBWrapper empty_wrapper = new TransDBWrapper(replica, op.trans_id);
            OccExecutor executor = new OccExecutor(empty_wrapper, this);
            executor_trans_map.put(op.trans_id, executor);
        }
        OccExecutor exist_executor = executor_trans_map.get(op.trans_id);
        List<String> result = new ArrayList<>();

        for (String key : op.getFields()){
            result.add(exist_executor.cache.read(key));
        }
        return String.join(",", result);
    }

    public void create_or_update_executor_by_write_op(YCSBMessage op){
        if (!this.executor_trans_map.containsKey(op.trans_id)){ // already exists
            TransDBWrapper empty_wrapper = new TransDBWrapper(replica, op.trans_id);
            OccExecutor executor = new OccExecutor(empty_wrapper, this);
            executor_trans_map.put(op.trans_id, executor);
        }
        OccExecutor exist_executor = executor_trans_map.get(op.trans_id);
        exist_executor.cache.wtite(op.getKey(), new String(op.getValues().get(op.getKey())));
    }

    public boolean create_or_update_executor_by_commit_op(YCSBMessage op){
        if (!this.executor_trans_map.containsKey(op.trans_id)){ // already exists
            TransDBWrapper empty_wrapper = new TransDBWrapper(replica, op.trans_id);
            OccExecutor executor = new OccExecutor(empty_wrapper, this);
            executor_trans_map.put(op.trans_id, executor);
        }
        OccExecutor exist_executor = executor_trans_map.get(op.trans_id);
        try {
            return exist_executor.validate();
        }catch (IOException e){
            System.out.println(e);
            return false;
        }
    }

    public TransDBWrapper get_cache_by_tn(int tn){
        assert(wrapper_trans_map.containsKey(tn)); // todo: try
        return wrapper_trans_map.get(tn);
    }

    public OccExecutor get_executor_by_id(int id){
        assert(executor_trans_map.containsKey(id)); // todo: try
        return executor_trans_map.get(id);
    }

    public boolean commit_after_reach_consensus(TransDBWrapper cache){
        assert(!wrapper_trans_map.containsKey(committed_trans_num)); // todo: try
        wrapper_trans_map.put(committed_trans_num, cache);
        executor_trans_map.remove(cache.trans_id);
        return cache.commit();

    }
}
