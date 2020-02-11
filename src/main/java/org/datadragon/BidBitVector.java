package org.datadragon;
import java.io.Serializable;

public class BidBitVector implements Serializable{
    public BitVector dn;
    public BitVector up;
    public BidBitVector(String bitVec, long bytes, String direction){
        if (direction.equals("toSub")){
            dn = new BitVector(bitVec, bytes);
            up = null;
        }else{
            dn = null;
            up = new BitVector(bitVec, bytes);
        }
    }

    public String toStr(){
        String dn_str = (dn==null)?"":dn.toStr();
        String up_str = (up==null)?"":up.toStr();
        return dn_str+"#"+up_str;
    }

    public BidBitVector add(BidBitVector other){
        if (this.dn!=null && other.dn!=null) this.dn.add(other.dn);
        else if (other.dn!=null) this.dn = other.dn;

        if (this.up!=null && other.up!=null) this.up.add(other.up);
        else if (other.up!=null) this.up = other.up;
        return this;
    }

    public double dnTput0(){return (dn==null)?0:dn.tput0();}
    public double dnTput1(){return (dn==null)?0:dn.tput1();}
    public double dnTputMax(){return (dn==null)?0:dn.tputMax();}

    public double upTput0(){return (up==null)?0:up.tput0();}
    public double upTput1(){return (up==null)?0:up.tput1();}
    public double upTputMax(){return (up==null)?0:up.tputMax();}

    public static void main(String[] args){
        BidBitVector a = new BidBitVector("0000000000000000000000000000000000000000000000000000001F0000", 823424, "toSub");
        System.out.println(a.toStr());
        BidBitVector b = new BidBitVector("0000000000000000000000000000000000003D0000000000000000000000", 434245, "toSub");
        System.out.println(b.toStr());
        a = a.add(b);
        System.out.println(a.toStr());
        System.out.println((a.dnTput0()) + "," + a.dnTput1() + "," + a.dnTputMax());
        System.out.println((a.upTput0()) + "," + a.upTput1() + "," + a.upTputMax());
    }

}
