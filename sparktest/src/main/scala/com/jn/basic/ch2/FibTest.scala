package com.jn.basic.ch2

/**
 * Created by root on 16-7-4.
 */
object FibTest {
    def fib(n: Int): Int ={
        if(n ==0) 0//如果输入值为0返回值为0，
        else if(n==1) 1//如果输入值为1返回为1
        else{
            fib(n-1)+fib(n-2)//后一个值是前两个值的和
        }
    }
    def main(args: Array[String]) {
        for(i <- 0 to 10){
            println(fib(i))
        }
    }
}
/**
0
1
1
2
3
5
8
13
21
34
55
  */