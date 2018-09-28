for i in `seq 1 16`; do echo -n "mac$i " && ssh nerv$i ~/git_repos/rocc/scripts/cat_hugepage.sh ; done
