## READ ME

### Download code
```shell
git clone https://github.com/leophan/o_apache_project_poc.git
```

### Setup
```shell
pip install virtualenv
# cd project_folder
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Submit job
```shell
bin/spark-submit \
    --master spark://domain-master:7077 \
    --py-files ./o_apache_project_poc/src/utils.py \
    ./o_apache_project_poc/src/main.py
```