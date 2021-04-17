BEGIN{cnt=0; sum=0; max=0;} 
/System throughput/{
    cnt+=1; 
    if($5~"K") {
        sum+=$4/1000.0; 
        if ($4/1000 > max) max = $4/1000;
    } else if($5~"M") {
        sum+=$4; 
        if ($4 > max) max = $4;
    }
}
END{if (cnt == 0) print 0; else print max}
