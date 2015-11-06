The data upload code is run via Google Compute Engine.  The main module is `uploadTCGA` and takes a config file for an argument

The compute engine is constructed by the following command:

`gcloud compute instances create <gce name> --zone us-central1-a --machine-type n1-standard-16 --image ubuntu-14-04 --project <project id> 
	--scope <GCP service account>=storage-full  <GCP service account>=bigquery 
	<GCP service account>=compute-rw <GCP service account>=logging-write <GCP service account>=sql 
	<GCP service account>=sql-admin --boot-disk-device-name upload-instance-16 --boot-disk-size 1000GB --boot-disk-type pd-standard`

Then it can be ssh'ed into: `gcloud compute --project <project id> ssh <gce name> --zone us-central1-a`

To initialize, run the following commands:

`sudo apt-get update `

`sudo apt-get install -y python-pip python-dev libffi-dev git-core psmisc tmux `

`sudo pip install -U pip `

`mkdir /tmp/pkgs `

`gsutil -m rsync gs://<project id>/<code folder>/pkgs/ /tmp/pkgs/ `

`sudo pip install /tmp/pkgs/* `

`sudo pip install git+https://github.com/craigcitro/apitools.git#egg=apitools `

`sudo pip install git+https://github.com/GoogleCloudPlatform/gcloud-python.git#egg=gcloud `

`sudo pip install futures requests pandas ipython lockfile tablib `

`sudo pip install -U crcmod`

`sudo apt-get install libxml2-dev libxslt-dev python-dev `

`sudo apt-get install lib32z1-dev`

`sudo apt-get install zip`

`sudo apt-get install -y zlib1g-dev`

`STATIC_DEPS=true sudo pip install lxml`

`sudo pip install gsutil `

`sudo apt-get install python-mysqldb`

`gs://<project id>/<code folder>/pkgs/` should contain the following packages:

`cffi-0.8.6-cp27-none-linux_x86_64.whl`

`numpy-1.9.1-cp27-none-linux_x86_64.whl`

`pycparser-2.10-py2-none-any.whl`

`six-1.9.0-py2.py3-none-any.whl`

`cryptography-0.7.1-cp27-none-linux_x86_64.whl`

`pandas-0.15.2-cp27-none-linux_x86_64.whl`

`python_dateutil-2.4.0-py2.py3-none-any.whl`

`enum34-1.0.4-py2-none-any.whl`

`pyasn1-0.1.7-py2-none-any.whl`

`pytz-2014.10-py2.py3-none-any.whl`

then clone the code from the `isb-cgc/isb-cgc-data-proc repository` to a folder
then cd into the data_upload folder

set up the python path:

`export ISBCGC_ROOT_DIR=<path to data_upload>`

`export PYTHONPATH=$ISBCGC_ROOT_DIR/main:$ISBCGC_ROOT_DIR/model:$ISBCGC_ROOT_DIR/util:$ISBCGC_ROOT_DIR/test:$ISBCGC_ROOT_DIR/cghub/python/main:$ISBCGC_ROOT_DIR/../cghub/python/util`


then run: `python main/uploadTCGA.py config/<config file name>`

