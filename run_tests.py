import subprocess
import os
import sys

def build_update(verbose=False):
    print "Pulling in latest updates and building"
    return run("./build.sh --update", verbose)

def vagrant_up(verbose=False):
    print "Bringing up vagrant cluster"
    return run("vagrant up --provider=aws --no-provision --no-parallel", verbose)

def vagrant_provision(verbose=False):
    print "Provisioning vagrant cluster"
    return run("vagrant provision", verbose)

def run_tests(verbose=False):
    return run("ducktape muckrake/tests/", verbose)

def vagrant_destroy(verbose=False):
    "Destroying vagrant cluster"
    return run("vagrant destroy -f", verbose)

def run(cmd, verbose):
    print "Running %s" % cmd
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(proc.stdout.readline, ''):
        if verbose:
            print line

    output, err = proc.communicate()
    print output
    return proc.returncode

def main():
    # Run commands in the right location
    muckrake_dir = os.path.abspath(os.path.dirname(__file__))
    os.chdir(muckrake_dir)

    # Pull in updates from github
    build_update()

    # Bring up cluster from scratch
    vagrant_up()
    vagrant_provision()

    exit_status = run_tests(True)

    #vagrant_destroy()
    sys.exit(exit_status)
    print exit_status

if __name__ == "__main__":
    main()
