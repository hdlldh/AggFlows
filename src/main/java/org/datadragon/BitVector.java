package org.datadragon;
import java.io.Serializable;

public class BitVector implements Serializable{
    private static final int n= 60;
    public byte[] activeVec = new byte[n];
    public double[] bytesVec =  new double[n];
    private static final double coef = 8.0/1024;

    public BitVector(String bitVec, long bytes){
        int size = 0;
        for (int i=0; i<n; i++){
            int idx = i + 1 - 2* (i%2);
            int num = Integer.parseInt(bitVec.substring(i,i+1), 16);
            String binNum = Integer.toBinaryString(num);
            for (char ch: binNum.toCharArray()){
                if (ch=='1'){
                    size++;
                    activeVec[idx]++;
                }
            }
        }
        for (int i=0; i<n; i++){
            if (activeVec[i]!=0){
                bytesVec[i] = ((double) bytes)/size * activeVec[i];
                activeVec[i] = 1;
            }

        }
    }

    public String toStr(){
        StringBuilder s = new StringBuilder();
        for (int i=0; i<n; i++){
            if (i!=0) s.append("_");
            s.append((int) (bytesVec[i] * coef) + ":" + activeVec[i]);
        }
        return s.toString();
    }

    public BitVector add(BitVector other){
        if (other==null) return this;
        for (int i=0; i<n; i++){
            this.activeVec[i] += other.activeVec[i];
            this.bytesVec[i] += other.bytesVec[i];
        }
        return this;
    }

    public int getBusySec() {
        int secs = 0;
        for (int i=0; i<n; i++){
            if (activeVec[i]!=0)  secs++;
        }
        return secs;
    }

    public int first(){
        int pos = 0;
        while (pos < n && activeVec[pos]==0) pos++;
        return pos;
    }

    public int last(){
        int pos = n-1;
        while (pos>=0 && activeVec[pos]==0) pos--;
        return pos;
    }

    public double tput0() {
        double b = 0;
        for (double e: bytesVec) b+= e;
        return b *coef/(last()-first()+1);
    }

    public double tput1(){
        double b = 0;
        for (double e: bytesVec) b+= e;
        return b *coef/getBusySec();
    }

    public double tputMax(){
        double b = 0;
        for (double e: bytesVec) b = Math.max(b, e);
        return b*coef;
    }

    public static void main(String[] args){
        BitVector a = new BitVector("0000000000000000000000000000000000000000000000000000001F0000", 823424);
        BitVector b = new BitVector("0000000000000000000000000000000000003D0000000000000000000000", 434245);
        a = a.add(b);
        System.out.println(a.toStr());
        System.out.println((a.first()) + "," + a.last() + "," + a.getBusySec());
        System.out.println((a.tput0()) + "," + a.tput1() + "," + a.tputMax());
    }


}
