package bftsmart.demo.currency_control;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TransDBWrapper {
    public Map<String, String> copies = new HashMap<>();
    public Set<String> read_set = new HashSet<>();
    public ReplicaDef replicaDef;
    public int trans_id;
    public TransDBWrapper(ReplicaDef replicaDef, int trans_id){
        this.replicaDef = replicaDef;
        this.trans_id = trans_id;
    }
    public String read(String key){ // todo: where to use read
        this.read_set.add(key);
        if (this.copies.containsKey(key)) {
            return this.copies.get(key);
        } else {
            return this.replicaDef.readFromReplicaLocalDB(key);
        }
    }

    public boolean wtite(String key, String val){
        this.copies.put(key, val);
        return true;
    }

    public void abort(){
        this.copies.clear();
        this.read_set.clear();
//        System.out.println("Abort succeed.");
    }

    public boolean commit(){
        for (String key : this.copies.keySet())
            this.replicaDef.writeToReplicaLocalDB(key,  this.copies.get(key));
        return true;
    }

    public Set<String> get_read_set() {
        return this.read_set;
    }

    public Set<String> get_write_set(){
        return this.copies.keySet();
    }

}