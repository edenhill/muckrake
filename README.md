# muckrake
Confluent Platform system tests
System Integration & Performance Testing
========================================

This repository contains system tests for the Confluent Platform as well as a few scripts to help
set up a Vagrant cluster provisioned with all of the repositories in the Confluent
Platform. The system tests should be run using ducktape (see https://github.com/confluentinc/ducktape/
for installation instructions).


Muckrake set up
---------------
In order to run the muckrake system tests using ducktape, a few preparatory steps are necessary.

* Get muckrake and ducktape (the base directory doesn't matter):
``` 
    git clone git@github.com:confluentinc/muckrake.git
    git clone git@github.com:confluentinc/ducktape.git
    cd ducktape && python setup.py install # Now you can run ducktape!
```


* Download and install the Confluent projects within muckrake directory. This provides a mechanism to help provision the machines in your Vagrant cluster with Confluent binaries, scripts etc. (If you already initialized the repositories and want to update, instead run `./build.sh --update && vagrant rsync`)
``` 
    cd muckrake
    ./build.sh # check out and build Confluent projects against specified version
```
   
* Configure Vagrant by creating the file `Vagrantfile.local`. At minimum this needs `enabled_dns = true` and `num_workers = <enough workers to run your tests>`.
```
    # example Vagrantfile.local
    enable_dns = true
    num_workers = 7  # This should be set high enough
```

* Ensure your Vagrant installation has the necessary plugins.
```
    # Check that vagrant-hostmanager is in your list of installed plugins
    vagrant plugin list | grep vagrant-hostmanager
    
    # If it's not there, you'll need to install:
    vagrant plugin install vagrant-hostmanager
```

* Bring up the Vagrant cluster
```
    # Run this command if you're testing on your local machine
    vagrant up 
    
    # Run this command if you're on aws
    # vagrant provision is only necessary the first time you bring up your Vagrant cluster
    vagrant up --provider=aws --no-parallel && vagrant provision
    
```

Run tests
---------
* Run one or more tests with ducktape. 
```    
    # Run tests in a file
    duckape muckrake/tests/test_my_system_test.py
    
    # Run tests in a directory (search recursively)
    ducktape muckrake/tests
    
    # See which tests will be run without actually running any
    ducktape muckrake/tests --collect-only
```

Summary results are printed to screen and output into `./results`. The most recent test results are sym-linked from `./results/latest`.

If you find that tests are never getting loaded, you may be experiencing https://github.com/confluentinc/ducktape/issues/32. Some users have trouble with the test loader due to the muckrake directory not being in the module search path (`sys.path`). As a temporary work-around, add this line to your bashrc: `export PYTHONPATH=$PYTHONPATH:<path_to_muckrake>`


Adding New Repositories
-----------------------

If your new service requires a new code base to be included, you may also need
to modify the `build.sh` script. Ideally you just need to add a new line like

    build_maven_project "kafka-rest" "git@github.com:confluentinc/kafka-rest.git" "package"

at the bottom of the script. The generic `build_maven_project` function uses the
given directory name, git repository, and Maven action to check out and build
the code. Any code under the repository is automatically pushed to worker nodes
and made available under `/vagrant`. (You'll need to use the `"install"` action
if the repository is for a shared library since subsequent packages will need to
be able to find it in the local Maven repository. You probably also want to
update the `.gitignore` file to ignore the new subdirectory and
`vagrant/base.sh` to provide a symlink under `/opt` in addition to the code
under `/vagrant` to get Vagrant-agnostic naming.

EC2 Quickstart
--------------

* Start jump server instance (manually through the AWS management console)
 - If you haven't already, setup a keypair to use for SSH access.
 - Nothing too big, t2.small or t2.medium is easily enough since this machine is
   just a driver. t2.micro is even usable, but you do have to do builds locally
   so you might want something a bit bigger.
 - Ubuntu is recommended (only because the initial setup has been documented and
   tested below)
 - Most defaults are fine. Make sure you get an auto-assigned public IP
   (normally on for the default settings) and set IAM role ->
   ducktape-master, which gives permissions to launch/kill additional
   machines. Tagging the instance with a useful name,
   e.g. "<you>-ducktape-master" is recommended. Set the security group to
   'ducktape-insecure', which leaves the machine open on a variety of ports for
   traffic coming from other machines in the security group and enables SSH
   access from anywhere. This is less secure than a production config, but makes
   it so we generally don't have to worry about adding ports whenever we add new
   services to ducktape.
 - Once started, grab the public hostname/IP and SSH into the host using
   `ssh -i /path/to/keypair.pem ubuntu@public.hostname.amazonaws.com`. All
   remaining steps will be run on the jump server. Highly recommended: use tmux 
   so any connectivity issues don't kill your session
   (installation - mac: brew install tmux; ubuntu/debian: sudo apt-get install tmux). 
 - Put a copy of the SSH key you're using (the pem file) on the jump server using 
   `scp -i /path/to/confluent.pem /path/to/confluent.pem ubuntu@public.hostname.amazonaws.com:confluent.pem`

* Start by making sure you're up to date and installing a few dependencies,
  getting ducktape, and building:

        sudo apt-get update && sudo apt-get -y upgrade && sudo apt-get install -y git
        git clone https://github.com/confluentinc/ducktape.git
        cd ducktape
        aws/aws-init.sh
        . ~/.bashrc

  Now is a good time to install any extra stuff you might need, e.g. your
  preferred text editor.

* An example Vagrantfile.local has been created by aws-init.sh which looks something like:

        ec2_instance_type = "..." # Pick something appropriate for your
                                  # test. Note that the default m3.medium has
                                  # a small disk.
        enable_dns = true
        num_workers = 4
        ec2_keypair_name = 'confluent'
        ec2_keypair_file = '/home/ubuntu/confluent.pem'
        ec2_security_groups = ['ducktape-insecure']
        ec2_region = 'us-west-2'
        ec2_ami = "ami-29ebb519"

  These settings work for the default AWS setup you get with Confluent's
  account (of course `num_workers`, `ec2_keypair_name` and `ec2_keypair_file`
  should all be customized for your setup).

* Start up the instances:

        vagrant up --provider=aws --no-provision --no-parallel && vagrant provision

* Now you should be able to run tests:

        python -m ducktape.tests.native_vs_rest_performance

* Once configured, you can just shutdown your machines until you need them
  again. To do so, use

        vagrant halt

  to stop your cluster and manually stop your jump server from the management
  console. To bring them back up, restart your jump server manually, then log in
  and run

         vagrant up --provider=aws --no-provision && vagrant provision

  to restore the cluster. You need to reprovision to fix up some permissions for
  ephemeral drives (under `/mnt`) which are lost when you halt the VMs, but it
  should be very fast since most of the provisioning work is maintained across
  reboots.
