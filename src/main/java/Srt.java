import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Iterator;

public class Srt  {
        public static List<String> sort(Map<String, Integer> hm){
                List<String> list = new LinkedList<String>();
                int c =0;
                while(hm.size()>0){
                        Iterator<String> it = hm.keySet().iterator();
                        String max = "";
                        int cnt = 0 ;
                        while(it.hasNext()){
                                String key = it.next();
                                if(cnt==0){
                                        max = key;
                                        cnt=1;
                                }
                                if(hm.get(key)>hm.get(max)){
                                        max=key;
                                }
                        }
                        list.add(max);
                        c++;
                        hm.remove(max);
                }
                return list;
        }


}
