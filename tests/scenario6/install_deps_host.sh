add-apt-repository ppa:deadsnakes/ppa -y
apt-get update
apt-get install python3.6 -y
apt install python3-pip -y
apt install python3.6-dev -y


apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common -y
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install docker-ce docker-ce-cli containerd.io -y
apt install docker-compose -y

/usr/bin/python3.6 -m pip install matplotlib
/usr/bin/python3.6 -m pip install https://github.com/Iftimie/P2P-RPC.git@feature/add_docker

cd ../..; sudo /usr/bin/python3.6 setup.py install; cd tests/scenario6
echo "export PATH=\"`/usr/bin/python3.6 -m site --user-base`/bin:$PATH\"" >> ~/.bashrc

pip3 install matplotlib