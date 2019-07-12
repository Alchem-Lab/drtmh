#!/bin/bash
show(){
  #echo "$2"
  echo -e "\033[41;37m $2 \033[0m"
  cat sundial$1_0 | grep -i "$2" | wc -l
  cat sundial$1_1 | grep -i "$2" | wc -l
  echo ""
}
succ(){
  cat sundial$1_0 | grep -i "succs ratio" 
  cat sundial$1_1 | grep -i "succs ratio" 
  echo ""
}
echo ""
show $1 "start workload"
show $1 "finish workload"
show $1 "handler"
show $1 "send back"
show $1 "add to wait"
show $1 "end timer"
show $1 "in wait success send back to"
succ $1
