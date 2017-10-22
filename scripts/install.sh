scp -P 2122 target/stat-1.0-SNAPSHOT.jar root@127.0.0.1:/tmp
ssh -p 2122 root@127.0.0.1 docker cp /tmp/stat-1.0-SNAPSHOT.jar sandbox:/tmp
ssh -p 2122 root@127.0.0.1 docker cp /tmp/stat-1.0-SNAPSHOT.jar sandbox:/usr/hdp/current/accumulo-tablet/lib/ext
