1) Use the config.sh script to set up a virtual
environment:

$ source config.sh

You can then run the recv.py from within this environment.  Remember
to use the 'deactivate' command to exit the environment when done!

2) python  ./protonclientgnocchi/recv.py -a localhost:5672 -m 0
