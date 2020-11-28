cd ~

sudo yum install -y epel-release
sudo yum install -y gcc git htop iotop atop sysstat wget nano make

cd ~
wget https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh
mv git-prompt.sh .git-prompt.sh
cat <<EOF | tee -a ~/.bashrc
source ~/.git-prompt.sh
PS1='\[\033[0;34m\]testgcp1\[\033[0;33m\] \w\[\033[00m\]\$(__git_ps1)\n> '
alias l="ls -la"
alias ..="cd .."
export DATADIR=/data
export DATADIREBS=/data-ebs
EOF

source ~/.bashrc

arch=""
case $(uname -m) in
    i386)   arch="386" ;;
    i686)   arch="386" ;;
    x86_64) arch="amd64" ;;
    arm)    dpkg --print-architecture | grep -q "arm64" && arch="arm64" || arch="arm" ;;
esac

cd ~
wget https://storage.googleapis.com/golang/go1.15.1.linux-$arch.tar.gz
sudo tar -C /usr/local -xzf go1.15.1.linux-$arch.tar.gz
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

echo 100 | sudo tee /proc/sys/vm/dirty_expire_centisecs
echo 100 | sudo tee /proc/sys/vm/dirty_writeback_centisecs
echo 50 | sudo tee /proc/sys/vm/dirty_background_ratio
echo 80 | sudo tee /proc/sys/vm/dirty_ratio

echo "10.150.0.9 shackle-1" | sudo tee -a /etc/hosts
echo "10.150.0.10 shackle-2" | sudo tee -a /etc/hosts
echo "10.150.0.8 shackle-3" | sudo tee -a /etc/hosts

git init --bare shackle.git
git clone shackle.git

exit

# Copy/paste this
#---

sudo yum install -y git wget
git clone https://github.com/kevburnsjr/badger-bench
cd ~/badger-bench
chmod +x init.sh
./init.sh

sudo mkfs -t ext4 /dev/nvme0n1
sudo mkdir /data
sudo mount /dev/nvme0n1 /data
sudo chown centos:centos /data

#---
# end

# GCP

sudo mkfs -t ext4 /dev/nvme0n1
sudo mkdir /data
sudo mount /dev/nvme0n1 /data
sudo mkdir /data/node
sudo chown -R kevburnsjr:kevburnsjr /data

sudo mkdir -p /data-ebs/raft
sudo chown -R kevburnsjr:kevburnsjr /data-ebs

sudo mkdir /data2
sudo mount /dev/nvme0n2 /data2
sudo chown kevburnsjr:kevburnsjr /data2

sudo mkdir -p /data/test1
sudo mkdir /data1/test1
sudo mkdir /data2/test1
sudo chown kevburnsjr:kevburnsjr /data/test1
ln -s /data1/test1 /data/test1/0
ln -s /data2/test1 /data/test1/1

#---

sudo mkfs -t ext4 /dev/sdb
sudo mkdir /data
sudo mount /dev/sdb /data

sudo mkfs -t ext4 /dev/sdc
sudo mkdir /data-nvme
sudo mount /dev/sdc /data-nvme


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

# i3.2xl nvme partitions
sudo mkfs -t ext4 /dev/nvme0n1p1
sudo mkfs -t ext4 /dev/nvme0n1p2
sudo mkfs -t ext4 /dev/nvme0n1p3
sudo mkfs -t ext4 /dev/nvme0n1p4
sudo mkdir -p /data/0
sudo mkdir -p /data/1
sudo mkdir -p /data/2
sudo mkdir -p /data/3
sudo mount /dev/nvme0n1p1 /data/0
sudo mount /dev/nvme0n1p2 /data/1
sudo mount /dev/nvme0n1p3 /data/2
sudo mount /dev/nvme0n1p4 /data/3
sudo chown ec2-user:ec2-user /data/0
sudo chown ec2-user:ec2-user /data/1
sudo chown ec2-user:ec2-user /data/2
sudo chown ec2-user:ec2-user /data/3

# Local

sudo mkfs -t ext4 /dev/sdb
sudo mkdir /data
sudo mount /dev/sdb /data

sudo mkfs -t ext4 /dev/sdc
sudo mkdir /data-magnetic
sudo mount /dev/sdc /data-magnetic

# Centos 8
sudo mkfs -t ext4 /dev/nvme0n1
sudo mkdir /data
sudo mount /dev/nvme0n1 /data
sudo chown -R centos:centos /data
