package com.jn.spark.core

/**
 * Created by root on 16-6-22.
 */
class SecondSortKey(val first: Int,val second: Int) extends Ordered[SecondSortKey] with Serializable{
    override def compare(other: SecondSortKey): Int = {
        if(this.first-other.first != 0){
            this.first - other.first
        }else{
            this.second - other.second
        }
    }
}

