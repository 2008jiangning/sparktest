/**
 * Created by root on 16-7-1.
 */
public class MaoPaoSortTest {
    public static void sort(int[] arr){
        for(int i=0;i<arr.length-1;i++){//最后一个元素不用进行排序
            for(int j=0;j<arr.length-i-1;j++){//从第一个元素到已经排序个数
                 if(arr[j]>arr[j+1]){//第一关与第二个进行比较
                     int temp = arr[j];//临时变量存大的
                     arr[j] = arr[j+1];//
                     arr[j+1] = temp;
                 }
            }
        }
    }
    public static void main(String[] args){
        int[] arr = {3,8,5,4};
        sort(arr);
        for(int o : arr){
            System.out.println(o);
        }
    }
}
