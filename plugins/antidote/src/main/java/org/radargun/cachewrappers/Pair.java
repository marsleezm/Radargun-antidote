package org.radargun.cachewrappers;

public class Pair {
    public int fst; //first member of pair
    public int snd; //second member of pair

    public Pair(int first, int second) {
        this.fst = first;
        this.snd = second;
    }
    
    public boolean equals(Object o){
    	if (o instanceof Pair)
    		return ((Pair) o).fst==fst && ((Pair) o).snd==snd;
    	else
    		return false;
    }
    
    public int hashCode(){
    	return fst*10+snd;
    }
}
