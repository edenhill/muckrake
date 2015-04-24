import subprocess
import os

def build_update():
    return run("./build.sh --update")

def vagrant_up():
    return run("vagrant up --provider=aws --no-provision --no-parallel")

def vagrant_provision():
    return run("vagrant provision")

def run_tests():
    return run("export PYTHONPATH=$PYTHONPATH:/muckrake && ducktape muckrake/tests/everything_runs_test.py")

def vagrant_destroy():
    return run("vagrant destroy -f")

def run(cmd):
    print "Running %s" % cmd
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(p.stdout.readline, ''):
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

    exit_status = run_tests()

    vagrant_destroy()
    print exit_status
    sys.exit(exit_status)

if __name__ == "__main__":
    main()
