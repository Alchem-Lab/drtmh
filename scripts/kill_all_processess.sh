for mac in $@; do
    echo "kill processes in $mac."
    ssh $mac /home/chao/git_repos/rocc/scripts/kill_processes.sh
done

echo "kill processes in `hostname`."
/home/chao/git_repos/rocc/scripts/kill_processes.sh
