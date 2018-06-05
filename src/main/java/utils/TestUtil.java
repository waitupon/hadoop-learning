package utils;

public class TestUtil {
    public static void main(String[] args) {
        int num = 3;
        int step = 10 / num;
        int[][] arr = new int[num][2];

        for(int i=0; i<num; i++){
            if(i==0){
                arr[i] = new int[]{Integer.MIN_VALUE, 1901 + (i+1) * step};
            }else if(i == num -1){
                arr[i] = new int[]{1901 + i * step , Integer.MAX_VALUE};
            }else{
                arr[i] = new int[]{1901 + i * step , 1901 + (i+1) * step};
            }
        }
        int year = 1904;

        for(int i=0; i<num; i++){
            int[]ar = arr[i];
            if(year >=ar[0] && year < ar[1]){
                System.out.println(i);
            }
        }
    }
}
