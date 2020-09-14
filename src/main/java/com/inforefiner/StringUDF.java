package com.inforefiner;

public class StringUDF {

    public static void main(String[] args){

//        String string = "1,2,3 0,,ddas,NULL,DAS,132";
//        String separator = ",";
//        int num = 1;
//        StringUDF udf = new StringUDF();
//        String res = udf.eval(string, separator, num);
        String str = "fas,erw,dfs,afsdf";
//        int index = str.indexOf("1", 4);
        String s = str.substring(0,-1);
        System.out.println(s);



    }

    public String eval(String string, String separator, int num){
        int count = 0;//       0 1
        int nextIndex = 0;//   1
        int fromIndex = 0;//   2
        int subStrIndex = 0;// 0 2
        while (count != num){
            subStrIndex = fromIndex;
            nextIndex = string.indexOf(separator, fromIndex);
            fromIndex = nextIndex + 1;
            count++;
        }
        System.out.println("method: " + subStrIndex + "--" + nextIndex);
        String res = string.substring(subStrIndex, nextIndex);
        return res;
    }

}
