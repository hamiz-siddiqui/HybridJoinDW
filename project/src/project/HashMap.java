package project;
import java.util.Set;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections4.*;
import org.apache.commons.collections4.multimap.*;


public class HashMap {
	MultiValuedMap<Integer, Transactions> ht;
	int size;

	public HashMap(int s) {
		ht = new ArrayListValuedHashMap<Integer, Transactions>(s);
		size = s;
	}
	public HashMap() {
		ht = new ArrayListValuedHashMap<Integer, Transactions>();
		size = 0;
	}
	public void add(Transactions t) {
		ht.put(t.pid, t);
		size++;
	}
	public void drop(Transactions t) {
		ht.remove(t.pid);
		size--;
	}
	public void clear() {
		ht.clear();
	}
	public int getsize() {
		return ht.size();
	}
	public Set<Integer> getKeys(){
		return ht.keySet();
	}
//	public MasterData getMD(int pid){
//		Collection<MasterData> c = ht.get(pid);
//		Iterator <MasterData> i = c.iterator();
//		return i.next();	//Only one element of master data
//	}
}
