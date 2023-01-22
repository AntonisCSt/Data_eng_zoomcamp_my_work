### Environment setup 

For the course you'll need:

* Python 3 (e.g. installed with Anaconda)
* Google Cloud SDK
* Docker with docker-compose
* Terraform

If you have problems setting up the env, you can check this video:

* [Setting up the environment on cloud VM](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  * Generating SSH keys
We create SSH keys in order to connect to the GCP accountS

  * Creating a virtual machine on GCP

Created in GCP an instance (linux based)

  * Connecting to the VM with SSH

To connect we used `ssh -i $SSH_PATH antonis_hotmail@$EXTERNAL_GCP_VM_INSTANCE_IP`

  * Installing Anaconda

There we `bash $ANACONDA_LINK_LINUX_VERSION.sh`

  * Installing Docker

we first update the apt-get `sudo apt-get update`
and then we install docker `sudo apt-get install docker.io`

  * Creating SSH `config` file

We created config file `touch config` with Host and other details.
To run it we used: `ssh de-zoomcamp`

  * Accessing the remote machine with VS Code and SSH remote
  * Installing docker-compose
  * Installing pgcli
  * Port-forwarding with VS code: connecting to pgAdmin and Jupyter from the local computer
  * Installing Terraform
  * Using `sftp` for putting the credentials to the remote machine
  * Shutting down and removing the instance