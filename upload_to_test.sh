rm -rf bin
rm -rf /home/lutty/workspace/Go/GOPATH/src/github.com/harmony-one/harmony-me
cp -r ~/GolandProjects/harmony-me /home/lutty/workspace/Go/GOPATH/src/github.com/harmony-one/harmony-me
cd /home/lutty/workspace/Go/GOPATH/src/github.com/harmony-one/harmony-me
rm -rf vendor
make linux_static
cp -r bin ~/GolandProjects/harmony-me/bin
cd ~/GolandProjects/harmony-me
cd bin
gzip -9 harmony
scp -i /home/lutty/.ssh/other -P 15963 harmony.gz ubuntu@127.0.0.1:~/harmony.gz
#scp -i /home/lutty/.ssh/other -P 18567 harmony.gz ubuntu@127.0.0.1:~/harmony.gz

ssh -i /home/lutty/.ssh/other -p 15963 ubuntu@127.0.0.1 "gzip -d -f ~/harmony.gz"
#ssh -i /home/lutty/.ssh/other -p 18567 ubuntu@127.0.0.1 "gzip -d -f ~/harmony.gz"