cd ~

sudo yum install -y gcc-c++ git htop iotop atop snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel sysstat

sudo mkdir /data-ebs
sudo chown ec2-user:ec2-user /data-ebs

wget https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh
mv git-prompt.sh /home/ec2-user/.git-prompt.sh
cat <<EOF | tee -a ~/.bashrc
source ~/.git-prompt.sh
PS1='\[\033[0;34m\]loadtest\[\033[0;33m\] \w\[\033[00m\]\$(__git_ps1)\n> '
alias l="ls -la"
alias ..="cd .."
export DATADIR=/data
export DATADIREBS=/data-ebs
EOF

export ROCKSVERSION=5.1.4
wget https://github.com/facebook/rocksdb/archive/v$ROCKSVERSION.tar.gz
tar -xzvf v$ROCKSVERSION.tar.gz
cd rocksdb-$ROCKSVERSION
export USE_RTTI=1 && make shared_lib
sudo make install-shared
sudo ldconfig # to update ld.so.cache
echo "export LD_LIBRARY_PATH=/home/ec2-user/rocksdb-$ROCKSVERSION/" >> ~/.bashrc

source ~/.bashrc

arch=""
case $(uname -m) in
    i386)   arch="386" ;;
    i686)   arch="386" ;;
    x86_64) arch="amd64" ;;
    arm)    dpkg --print-architecture | grep -q "arm64" && arch="arm64" || arch="arm" ;;
esac

cd ~
wget https://storage.googleapis.com/golang/go1.13.7.linux-$arch.tar.gz
sudo tar -C /usr/local -xzf go1.13.7.linux-$arch.tar.gz
sudo ln -s /usr/local/go/bin/go /usr/bin/go
sudo mkdir /usr/local/share/go
sudo mkdir /usr/local/share/go/bin
sudo chmod 777 /usr/local/share/go

cd ~/badger-bench
go mod init github.com/dgraph-io/badger-bench

echo '
fs.file-max = 999999
net.ipv4.tcp_rmem = 4096 4096 16777216
net.ipv4.tcp_wmem = 4096 4096 16777216
' | sudo tee -a /etc/sysctl.conf
echo '1024 65535' | sudo tee /proc/sys/net/ipv4/ip_local_port_range
echo '
*               -       nofile         999999
' | sudo tee -a /etc/security/limits.conf

exit

# Copy/paste this
sudo yum install -y git
git clone https://github.com/kevburnsjr/badger-bench
cd ~/badger-bench
chmod +x init.sh
./init.sh

sudo mkfs -t ext4 /dev/nvme0n1
sudo mkdir /data
sudo mount /dev/nvme0n1 /data
sudo chown ec2-user:ec2-user /data


sudo mkfs -t ext4 /dev/sdb
sudo mkdir /data
sudo mount /dev/sdb /data

sudo mkfs -t ext4 /dev/xvdf
sudo mkdir /data-ebs
sudo mount /dev/xvdf /data-ebs
sudo chown ec2-user:ec2-user /data-ebs

sudo mkfs -t ext4 /dev/nvme1n1
sudo mkdir /data-ebs-2
sudo mount /dev/nvme1n1 /data-ebs-2
sudo chown ec2-user:ec2-user /data-ebs-2

driveletters=( f g h i j k l m n o p q r s t u )
for i in {0..15}
do
	h=$(printf "%x" $i)
	d="${driveletters[$i]}"
	sudo umount /data/$h
	sudo mkfs -t ext4 /dev/xvd$d
	sudo mount /dev/xvd$d /data/$h
	sudo chown ec2-user:ec2-user /data/$h
done

# a1
sudo mkfs -t ext4 /dev/nvme2n1
sudo mkdir /data-ebs
sudo mount /dev/nvme2n1 /data-ebs
sudo chown ec2-user:ec2-user /data-ebs

